import asyncio
import json
import logging
import os
import random
import signal
import sqlite3
import threading
import time
import secrets
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from curl_cffi import requests as cffi_requests
from rate_limits import EgressRatePolicy, classify_upstream_429
from proxy_utils import normalize_proxy_url
import proxy_pool
from rotator import flow_lock, active_flows_count, get_public_ip, get_ip_location, rotation_count
import panel_manager as panel_mgr

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
PAGE_SIZE = 5
IP_HISTORY_LIMIT = PAGE_SIZE * 5
BACKOFF_CAP = 30
POLL_ATTEMPTS = 6
WARP_ROTATION_ATTEMPTS = 4
WARP_POST_ROTATION_SLEEP = 3
DEFAULT_PROMPT_TOKENS = 50
DEFAULT_COMPLETION_TOKENS = 100
STREAM_CHUNK_SIZE = 4096
MODEL_DISCOVERY_INTERVAL = 300
DASHBOARD_REFRESH_INTERVAL = 3
STARTUP_TIME = time.time()
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "false").lower() in ("true", "1", "yes")
STREAM_TIMEOUT = 60

# Per-model timeout overrides (seconds) for known slow models
MODEL_TIMEOUT_OVERRIDES = {
    "hy3-free": 30,  # hy3-free is very slow (~90s), cut it short
}
# Reasoning-heavy free models can consume a tiny Anthropic max_tokens budget
# before emitting visible text. Keep the compatibility endpoint usable for
# short connection tests while allowing deployments to override the floor.
ANTHROPIC_MIN_MAX_TOKENS = max(0, int(os.environ.get("ANTHROPIC_MIN_MAX_TOKENS", "128")))
FLOW_LEASE_TTL_SECONDS = int(os.environ.get("FLOW_LEASE_TTL_SECONDS", "90"))
FLOW_LEASE_HEARTBEAT_SECONDS = int(os.environ.get("FLOW_LEASE_HEARTBEAT_SECONDS", "15"))
# SSE 批量取行：原来每行一次 run_in_executor，1 核机上全是 GIL 切换开销
STREAM_FETCH_BATCH = max(1, int(os.environ.get("STREAM_FETCH_BATCH", "32")))


def _drain_line_batch(get_next_line, line_iter, limit: int):
    out = []
    for _ in range(limit):
        try:
            item = get_next_line(line_iter)
        except Exception:
            break
        out.append(item)
        if item in ("STOP_ITERATION", "SOCKET_ERROR", None):
            break
    return out


async def _fetch_line_batch(loop, get_next_line, line_iter, limit: int):
    return await loop.run_in_executor(None, _drain_line_batch, get_next_line, line_iter, limit)


async def _post_upstream(session, url, *, json_body, headers, proxies, timeout):
    """同步 curl_cffi post 必须走线程池，否则阻塞事件循环拖住所有连接。"""
    return await asyncio.to_thread(
        session.post,
        url,
        json=json_body,
        headers=headers,
        impersonate="chrome124",
        stream=False,
        proxies=proxies,
        timeout=timeout,
    )


async def _post_upstream_stream(session, url, *, json_body, headers, proxies, timeout):
    return await asyncio.to_thread(
        session.post,
        url,
        json=json_body,
        headers=headers,
        impersonate="chrome124",
        stream=True,
        proxies=proxies,
        timeout=timeout,
    )

# -----------------------------------------------------------------------------
# JSON Structured Logging
# -----------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt or "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

# -----------------------------------------------------------------------------
# Proxy Pool / Custom Proxy List Support
# -----------------------------------------------------------------------------
PROXY_FILE = Path(os.environ.get("PROXY_LIST_FILE", "/app/data/proxies.txt"))
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()

def load_proxy_list():
    global _proxy_pool
    proxies = []
    if PROXY_FILE.exists():
        try:
            with open(PROXY_FILE, "r", encoding="utf-8") as f:
                lines = [normalize_proxy_url(line) for line in f if line.strip() and not line.startswith("#")]
                proxies.extend(lines)
        except Exception as e:
            log.error(f"Error reading proxies.txt: {e}")
    env_proxies = os.environ.get("PROXY_LIST", "").strip()
    if env_proxies:
        proxies.extend([normalize_proxy_url(p) for p in env_proxies.split(",") if p.strip()])
    _proxy_pool = list(dict.fromkeys(proxies))
    if _proxy_pool:
        log.info(f"Loaded {len(_proxy_pool)} custom proxies into pool.")

def get_next_outbound_proxy(session_key: str = "") -> Optional[Dict[str, str]]:
    """Return the current unified egress.

    A manual rotation can temporarily hold the route on mihomo while it walks
    through ROTATOR nodes. In that state do not immediately re-select a custom
    proxy on the next model request.

    If session_key is provided, the proxy is cached per-session so that all
    requests in the same conversation share the same egress.
    New sessions are assigned to idle (unbound) proxies when available.
    """
    custom_proxy = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()
    route = proxy_pool.routing_snapshot()

    # session 绑定：同一会话复用同一代理
    if session_key:
        bound_proxy = _session_proxy_get(session_key)
        if bound_proxy:
            # 检查绑定的代理是否仍可用
            if proxy_pool.is_proxy_eligible(bound_proxy):
                return {"http": bound_proxy, "https": bound_proxy}
            else:
                # 代理不可用，清除绑定
                _session_proxy_pop(session_key)
                log.debug(f"Session {session_key[:20]}... bound proxy {bound_proxy} unavailable; will reselect.")

    if route.get("mode") == "mihomo" and route.get("hold_mihomo") is True:
        if custom_proxy:
            return {"http": custom_proxy, "https": custom_proxy}
        return None

    # 为新 session 选代理：优先选空闲（未被其他 session 绑定）的代理
    if session_key:
        with _session_proxy_map_lock:
            bound_proxies = {proxy for proxy, _ in _session_proxy_map.values()}
        all_eligible = proxy_pool.eligible_proxies()
        if all_eligible:
            # 空闲代理 = 可用但未被任何 session 绑定的
            free_proxies = [p for p in all_eligible if p not in bound_proxies]
            log.debug(f"Session {session_key[:20]}... proxy selection: {len(all_eligible)} eligible, {len(bound_proxies)} bound, {len(free_proxies)} free")
            if free_proxies:
                proxy_url = proxy_pool.select_active_proxy(free_proxies)
                log.debug(f"Session {session_key[:20]}... selected free proxy: {proxy_url}")
            else:
                # 所有可用代理都被绑定了，选一个负载最少的
                proxy_url = proxy_pool.select_active_proxy(all_eligible)
                log.debug(f"Session {session_key[:20]}... all proxies bound, selected: {proxy_url}")
            if proxy_url:
                _session_proxy_set(session_key, proxy_url)
                return {"http": proxy_url, "https": proxy_url}

    proxy_url = proxy_pool.select_active_proxy(_proxy_pool)
    if proxy_url:
        if session_key:
            _session_proxy_set(session_key, proxy_url)
        return {"http": proxy_url, "https": proxy_url}
    if custom_proxy:
        if route.get("mode") != "mihomo":
            proxy_pool.set_mihomo_active(reason="no eligible custom proxy", hold=False)
        return {"http": custom_proxy, "https": custom_proxy}
    return None


def _request_proxy_url(proxies: Optional[Dict[str, str]]) -> Optional[str]:
    return proxy_pool.proxy_from_mapping(proxies)


def _mark_request_proxy_success(proxies: Optional[Dict[str, str]]) -> None:
    proxy_pool.mark_proxy_success(_request_proxy_url(proxies))


def _mark_request_proxy_failure(proxies: Optional[Dict[str, str]]) -> None:
    proxy_url = _request_proxy_url(proxies)
    if proxy_url:
        count = proxy_pool.mark_proxy_failure(proxy_url)
        log.warning("Custom proxy %s failed; fail_count=%s and cooldown applied.", proxy_url, count)


# -----------------------------------------------------------------------------
# SQLite — WAL mode + retry for concurrent safety
# -----------------------------------------------------------------------------
DB_FILE = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))
_db_lock = threading.Lock()
_db_pragmas_applied = False


def _apply_db_pragmas(conn) -> None:
    """One-time WAL setup; per-connection PRAGMA on every open is a write txn."""
    global _db_pragmas_applied
    if not _db_pragmas_applied:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        _db_pragmas_applied = True


def _get_conn():
    conn = sqlite3.connect(str(DB_FILE), timeout=5)
    _apply_db_pragmas(conn)
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _get_conn_row():
    conn = sqlite3.connect(str(DB_FILE), timeout=5)
    conn.row_factory = sqlite3.Row
    _apply_db_pragmas(conn)
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _db_execute(statement: str, params=()):
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    conn.commit()
                    return cursor
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def _db_fetchall(statement: str, params=()) -> list:
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn_row()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    return cursor.fetchall()
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def _db_executemany(statement: str, rows) -> int:
    rows = list(rows)
    if not rows:
        return 0
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.executemany(statement, rows)
                    conn.commit()
                    return cursor.rowcount
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise
    return 0


# --- metrics write batching -------------------------------------------------
# 请求热路径只做内存聚合 + 入队，后台线程批量刷盘，避免每请求抢 _db_lock。
# lease 表保持同步写：rotator 跨进程读它来挡轮换，不能只放内存。
_pending_usage_rows: deque = deque(maxlen=5000)
_pending_history_rows: deque = deque(maxlen=5000)
_metrics_flush_stop = threading.Event()
_metrics_flush_thread_started = False


def _prune_request_history() -> None:
    try:
        cutoff = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time() - REQUEST_HISTORY_RETENTION_DAYS * 86400),
        )
        _db_execute(
            "DELETE FROM request_history WHERE created_at < ? OR timestamp < ?",
            (cutoff, cutoff),
        )
        _db_execute(
            "DELETE FROM request_history WHERE id NOT IN "
            "(SELECT id FROM request_history ORDER BY id DESC LIMIT ?)",
            (REQUEST_HISTORY_MAX_ROWS,),
        )
    except Exception:
        pass


def _flush_metrics_buffers() -> None:
    usage_batch = []
    history_batch = []
    while _pending_usage_rows and len(usage_batch) < METRICS_FLUSH_MAX_ROWS:
        try:
            usage_batch.append(_pending_usage_rows.popleft())
        except IndexError:
            break
    while _pending_history_rows and len(history_batch) < METRICS_FLUSH_MAX_ROWS:
        try:
            history_batch.append(_pending_history_rows.popleft())
        except IndexError:
            break
    if usage_batch:
        try:
            _db_executemany("""
                INSERT INTO model_usage (model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(model_name) DO UPDATE SET
                    requests = requests + excluded.requests,
                    prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                    completion_tokens = completion_tokens + excluded.completion_tokens,
                    total_tokens = total_tokens + excluded.total_tokens,
                    estimated_cost_usd = estimated_cost_usd + excluded.estimated_cost_usd,
                    updated_at = CURRENT_TIMESTAMP
            """, usage_batch)
        except Exception as e:
            log.error(f"Failed to flush usage buffer to SQLite: {e}")
    if history_batch:
        try:
            _db_executemany(
                """INSERT INTO request_history
                   (timestamp, model, proxy_ip, egress_ip, latency_ms, status, is_stream, attempt, error_msg)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                history_batch,
            )
        except Exception:
            pass
        _prune_request_history()


def _metrics_flush_loop() -> None:
    while not _metrics_flush_stop.is_set():
        _metrics_flush_stop.wait(METRICS_FLUSH_INTERVAL_SECONDS)
        try:
            _flush_metrics_buffers()
        except Exception:
            pass


def start_metrics_flusher() -> None:
    global _metrics_flush_thread_started
    if _metrics_flush_thread_started:
        return
    _metrics_flush_thread_started = True
    threading.Thread(target=_metrics_flush_loop, name="metrics-flusher", daemon=True).start()

def init_db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model_name TEXT PRIMARY KEY,
            requests INTEGER DEFAULT 0,
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS ip_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT,
            country TEXT,
            flag TEXT,
            timestamp TEXT,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS warp_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            success INTEGER,
            latency_ms REAL,
            old_ip TEXT,
            new_ip TEXT
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS active_flow_leases (
            lease_id TEXT PRIMARY KEY,
            expires_at REAL NOT NULL
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS request_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            model TEXT NOT NULL,
            proxy_ip TEXT,
            egress_ip TEXT,
            latency_ms REAL,
            status TEXT NOT NULL,
            is_stream INTEGER DEFAULT 0,
            attempt INTEGER DEFAULT 1,
            error_msg TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_request_history_created "
        "ON request_history(created_at)"
    )


def acquire_flow_lease() -> str:
    lease_id = uuid.uuid4().hex
    touch_flow_lease(lease_id)
    return lease_id


def touch_flow_lease(lease_id: str) -> None:
    _db_execute(
        "INSERT OR REPLACE INTO active_flow_leases (lease_id, expires_at) VALUES (?, ?)",
        (lease_id, time.time() + FLOW_LEASE_TTL_SECONDS),
    )


def release_flow_lease(lease_id: str) -> None:
    _db_execute("DELETE FROM active_flow_leases WHERE lease_id = ?", (lease_id,))

def record_request_history(
    model: str,
    proxy_ip: str = "",
    egress_ip: str = "",
    latency_ms: float = 0,
    status: str = "ok",
    is_stream: bool = False,
    attempt: int = 1,
    error_msg: str = "",
):
    """Buffer one request row; flushed in batches by _metrics_flush_loop."""
    try:
        _pending_history_rows.append(
            (
                time.strftime("%Y-%m-%d %H:%M:%S"),
                model,
                proxy_ip or "",
                egress_ip or "",
                latency_ms,
                status,
                1 if is_stream else 0,
                attempt,
                error_msg[:200] if error_msg else "",
            )
        )
    except Exception:
        pass


def log_ip_rotation_to_db(ip: str, country: str, flag: str, timestamp: str, reason: str):
    try:
        _db_execute(
            "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
            (ip, country, flag, timestamp, reason)
        )
        _db_execute(
            "DELETE FROM ip_history WHERE id NOT IN (SELECT id FROM ip_history ORDER BY id DESC LIMIT ?)",
            (IP_HISTORY_LIMIT,),
        )
    except Exception as e:
        log.error(f"Failed to log IP rotation to DB: {e}")

def load_ip_history_from_db() -> List[Dict[str, any]]:
    if not DB_FILE.exists():
        return []
    try:
        rows = _db_fetchall(
            "SELECT ip, country, flag, timestamp, reason FROM ip_history ORDER BY id DESC LIMIT ?",
            (IP_HISTORY_LIMIT,)
        )
        history = []
        for r in reversed(rows):
            history.append({
                "ip": r[0], "country": r[1], "flag": r[2],
                "timestamp": r[3], "reason": r[4]
            })
        return history
    except Exception as e:
        log.error(f"Error loading IP history from DB: {e}")
        return []

def load_metrics_from_db() -> Dict[str, Dict[str, any]]:
    if not DB_FILE.exists():
        return {}
    try:
        rows = _db_fetchall(
            "SELECT model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd FROM model_usage"
        )
        stats = {}
        for r in rows:
            stats[r[0]] = {
                "requests": r[1], "prompt_tokens": r[2],
                "completion_tokens": r[3], "total_tokens": r[4],
                "estimated_cost_usd": r[5]
            }
        return stats
    except Exception as e:
        log.error(f"Error loading metrics from DB: {e}")
        return {}

# -----------------------------------------------------------------------------
# Prometheus Metrics
# -----------------------------------------------------------------------------
prom_requests_total = Counter("proxy_requests_total", "Total proxied requests", ["model", "endpoint"])
prom_requests_success = Counter("proxy_requests_success", "Successful proxied requests", ["model"])
prom_requests_rate_limited = Counter("proxy_requests_rate_limited", "Rate-limited requests", ["model"])
prom_rotation_count = Counter("proxy_rotations_total", "Total WARP rotations")
prom_active_flows = Gauge("proxy_active_flows", "Currently active streaming flows")
prom_request_duration = Histogram("proxy_request_duration_seconds", "Request duration", ["model", "endpoint"],
                                   buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0))
prom_warp_health = Gauge("proxy_warp_health", "WARP health (1=healthy, 0=unhealthy)")

# -----------------------------------------------------------------------------
# curl_cffi Session Pool
# -----------------------------------------------------------------------------
_session_pool: Dict[str, "SessionType"] = {}
_session_pool_lock = threading.Lock()
SessionType = None  # resolved at first use

def _get_session(endpoint: str):
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    with _session_pool_lock:
        if endpoint not in _session_pool:
            kwargs = {}
            if not ENABLE_HTTP2:
                from curl_cffi import CurlHttpVersion
                kwargs["http_version"] = CurlHttpVersion.V1_1
            # Disable low-speed limit (default 30s < 1byte/s) to avoid premature
            # timeout on slow upstream responses. The upstream timeout is handled
            # by the per-request timeout parameter instead.
            # Disable low-speed limit (default 30s < 1byte/s) to avoid premature
            # timeout on slow upstream responses. The upstream timeout is handled
            # by the per-request timeout parameter instead.
            kwargs["curl_options"] = {48: 0}  # LOW_SPEED_TIME=0 disables low-speed limit
            _session_pool[endpoint] = SessionType(**kwargs)
        return _session_pool[endpoint]

# --- stream session pool (per endpoint+egress, keepalive) --------------------
# 流式原来每次新建 Session（socks5h 下每次多一次 CONNECT+TLS 握手）。
# 按出口 key 做小池复用，失败才丢弃；成功用完归还，不断开 keepalive。
_stream_pool: Dict[str, list] = {}
_stream_pool_lock = threading.Lock()
STREAM_POOL_SIZE = max(1, int(os.environ.get("STREAM_POOL_SIZE", "2")))


def _stream_pool_key(endpoint: str, egress_key: str) -> str:
    return f"{endpoint}|{egress_key}"


def _get_stream_session(endpoint: str, egress_key: str):
    """Borrow a keepalive session for streaming; caller must release it."""
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    key = _stream_pool_key(endpoint, egress_key)
    with _stream_pool_lock:
        bucket = _stream_pool.get(key)
        if bucket:
            try:
                return bucket.pop(), key, False
            except IndexError:
                pass
    kwargs = {}
    if not ENABLE_HTTP2:
        from curl_cffi import CurlHttpVersion
        kwargs["http_version"] = CurlHttpVersion.V1_1
    kwargs["curl_options"] = {48: 0}
    return SessionType(**kwargs), key, True


def _release_stream_session(pool_key: str, session, discard: bool = False) -> None:
    if session is None:
        return
    if discard:
        try:
            session.close()
        except Exception:
            pass
        return
    with _stream_pool_lock:
        bucket = _stream_pool.setdefault(pool_key, [])
        if len(bucket) < STREAM_POOL_SIZE:
            bucket.append(session)
            return
    try:
        session.close()
    except Exception:
        pass

def _close_all_sessions():
    with _session_pool_lock:
        for ep, sess in _session_pool.items():
            try:
                sess.close()
            except Exception:
                pass
        _session_pool.clear()
    with _stream_pool_lock:
        for _key, bucket in _stream_pool.items():
            for sess in bucket:
                try:
                    sess.close()
                except Exception:
                    pass
        _stream_pool.clear()

def _invalidate_session(endpoint: str, expected_session=None):
    """Close and drop a pooled session so the next attempt builds a fresh one.

    ``curl_cffi`` does not expose a reliable public ``closed`` flag.  After an
    egress switch (or a transport error), simply calling ``session.close()``
    leaves the closed object in ``_session_pool``; the next retry then gets the
    same object and fails locally with ``SessionClosed`` without reaching the
    upstream at all.  ``expected_session`` also prevents one concurrent
    request from evicting a newer session installed by another request.
    """
    with _session_pool_lock:
        current = _session_pool.get(endpoint)
        if current is None:
            return
        if expected_session is not None and current is not expected_session:
            return
        sess = _session_pool.pop(endpoint, None)
    if sess is not None:
        try:
            sess.close()
        except Exception:
            pass


def _reset_request_session(endpoint: str, session, pooled: bool, stream_pool_key=None) -> None:
    """Make a request session safe to retry after a failed attempt."""
    if session is None:
        return
    if stream_pool_key is not None:
        _release_stream_session(stream_pool_key, session, discard=True)
        return
    if pooled:
        _invalidate_session(endpoint, expected_session=session)
        return
    try:
        session.close()
    except Exception:
        pass

_discovery_stop = threading.Event()

# -----------------------------------------------------------------------------
# Request Queue (drains during rotation)
# -----------------------------------------------------------------------------
_rotation_in_progress = threading.Event()
_request_drain_event = asyncio.Event()
_request_drain_event.set()

# 轮换超时与 drain：原来 rotate 35s + drain 15s，一次轮换冻结所有新请求至多 50s。
# rotator 侧单次 mihomo 验证约 2s + 两次外部 IP 查询（各 5~10s），10s 足够判定失败并重试。
ROTATE_REQUEST_TIMEOUT_SECONDS = max(5, int(os.environ.get("ROTATE_REQUEST_TIMEOUT_SECONDS", "10")))
ROTATION_DRAIN_TIMEOUT_SECONDS = max(2, int(os.environ.get("ROTATION_DRAIN_TIMEOUT_SECONDS", "5")))


async def wait_for_rotation_drain():
    if _rotation_in_progress.is_set():
        await asyncio.wait_for(_request_drain_event.wait(), timeout=ROTATION_DRAIN_TIMEOUT_SECONDS)

def signal_rotation_start():
    _rotation_in_progress.set()
    _request_drain_event.clear()

def signal_rotation_done():
    _rotation_in_progress.clear()
    _request_drain_event.set()

# -----------------------------------------------------------------------------
# Dual-WARP (active/passive tracking)
# -----------------------------------------------------------------------------
_dual_warp = {
    "active_ip": None,
    "standby_ip": None,
    "active_registration": "primary",
}
_dual_warp_lock = threading.Lock()

def swap_warp_registration():
    with _dual_warp_lock:
        _dual_warp["active_registration"] = (
            "standby" if _dual_warp["active_registration"] == "primary" else "primary"
        )
        return _dual_warp["active_registration"]

# -----------------------------------------------------------------------------
# Model pricing reference (USD per 1M tokens)
# -----------------------------------------------------------------------------
MODEL_PRICING = {
    "deepseek-v4-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.60},
    "mimo-v2.5-free": {"input_per_1m": 0.20, "output_per_1m": 0.80},
    "nemotron-3-ultra-free": {"input_per_1m": 0.25, "output_per_1m": 0.90},
    "laguna-s-2.1-free": {"input_per_1m": 0.20, "output_per_1m": 0.70},
    "hy3-free": {"input_per_1m": 0.15, "output_per_1m": 0.50},
    "muse-spark-1.2-contributor-free": {"input_per_1m": 0.10, "output_per_1m": 0.40},
    "nemotron-3.5-lightning-free": {"input_per_1m": 0.18, "output_per_1m": 0.60},
}

_model_usage_lock = threading.Lock()

# --- /metrics snapshot cache + egress status cache ---------------------------
# /metrics 原来每次都 doing DB 全表 + rotator HTTP + 外部 IP 库，被每秒抓取时就是烧 CPU。
_metrics_snapshot = None
_metrics_snapshot_at = 0.0
_metrics_snapshot_lock = threading.Lock()

# 出口状态由后台线程定期刷新（默认 60s），/health 与 /metrics 只读缓存，不再同步打外部站
_egress_cache = {"ok": True, "ip": None, "db_ok": True, "checked_at": 0.0}
_egress_cache_lock = threading.Lock()
EGRESS_STATUS_INTERVAL_SECONDS = max(15, int(os.environ.get("EGRESS_STATUS_INTERVAL_SECONDS", "60")))
_egress_watch_stop = threading.Event()
_egress_watch_started = False


def _read_egress_cache():
    with _egress_cache_lock:
        return dict(_egress_cache)


def _refresh_egress_cache() -> None:
    ok = False
    ip = None
    try:
        ip = get_public_ip()
        ok = bool(ip) and ip != "Disconnected"
    except Exception:
        ok = False
    db_ok = False
    try:
        _db_execute("SELECT 1")
        db_ok = True
    except Exception:
        pass
    with _egress_cache_lock:
        _egress_cache.update({"ok": ok, "ip": ip, "db_ok": db_ok, "checked_at": time.time()})
    try:
        prom_warp_health.set(1 if ok else 0)
    except Exception:
        pass


def _egress_watch_loop() -> None:
    _refresh_egress_cache()
    while not _egress_watch_stop.is_set():
        _egress_watch_stop.wait(EGRESS_STATUS_INTERVAL_SECONDS)
        try:
            _refresh_egress_cache()
        except Exception:
            pass


def start_egress_watcher() -> None:
    global _egress_watch_started
    if _egress_watch_started:
        return
    _egress_watch_started = True
    threading.Thread(target=_egress_watch_loop, name="egress-watcher", daemon=True).start()

def track_token_usage(model_name: str, prompt_tokens: int = 0, completion_tokens: int = 0):
    global model_usage_stats
    pricing = MODEL_PRICING.get(model_name, {"input_per_1m": 0.20, "output_per_1m": 0.80})
    prompt_cost = (prompt_tokens / 1_000_000) * pricing["input_per_1m"]
    completion_cost = (completion_tokens / 1_000_000) * pricing["output_per_1m"]
    cost = prompt_cost + completion_cost

    with _model_usage_lock:
        if model_name not in model_usage_stats:
            model_usage_stats[model_name] = {
                "requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                "total_tokens": 0, "estimated_cost_usd": 0.0
            }
        model_usage_stats[model_name]["requests"] += 1
        model_usage_stats[model_name]["prompt_tokens"] += prompt_tokens
        model_usage_stats[model_name]["completion_tokens"] += completion_tokens
        model_usage_stats[model_name]["total_tokens"] += (prompt_tokens + completion_tokens)
        model_usage_stats[model_name]["estimated_cost_usd"] += cost

    # 热路径只入队，后台线程批量刷盘；内存统计已实时更新，/metrics 不依赖本条落盘
    try:
        _pending_usage_rows.append(
            (model_name, 1, prompt_tokens, completion_tokens, prompt_tokens + completion_tokens, cost)
        )
    except Exception:
        pass

# -----------------------------------------------------------------------------
# WARP Quality Metrics
# -----------------------------------------------------------------------------
warp_quality_stats = {
    "total_attempts": 0, "successful_rotations": 0, "failed_rotations": 0,
    "last_latency_ms": 0.0, "avg_latency_ms": 0.0
}
_warp_quality_lock = threading.Lock()

def record_warp_rotation(success: bool, latency_ms: float = 0, old_ip: str = "", new_ip: str = ""):
    with _warp_quality_lock:
        warp_quality_stats["total_attempts"] += 1
        if success:
            warp_quality_stats["successful_rotations"] += 1
            warp_quality_stats["last_latency_ms"] = latency_ms
            n = warp_quality_stats["successful_rotations"]
            warp_quality_stats["avg_latency_ms"] = (
                (warp_quality_stats["avg_latency_ms"] * (n - 1) + latency_ms) / n
            )
        else:
            warp_quality_stats["failed_rotations"] += 1
    try:
        _db_execute(
            "INSERT INTO warp_quality (timestamp, success, latency_ms, old_ip, new_ip) VALUES (?, ?, ?, ?, ?)",
            (time.strftime("%Y-%m-%d %H:%M:%S"), 1 if success else 0, latency_ms, old_ip, new_ip)
        )
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Backoff helper
# -----------------------------------------------------------------------------
def compute_backoff_delay(attempt: int, base: float = 1.0, cap: int = BACKOFF_CAP) -> float:
    return min(base * (2 ** (attempt - 1)), cap) + random.uniform(0.5, 1.5)

# -----------------------------------------------------------------------------
# Jinja2 Templates
# -----------------------------------------------------------------------------
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# -----------------------------------------------------------------------------
model_usage_stats: Dict[str, Dict[str, float]] = {}

# Configuration & Dynamic Discovery
# -----------------------------------------------------------------------------
PORT = int(os.environ.get("OPENCODE_ZEN_PORT", "8000"))
HOST = os.environ.get("OPENCODE_ZEN_HOST", "127.0.0.1")
TARGET_ZEN_BASE = os.environ.get("OPENCODE_ZEN_TARGET_BASE", "https://opencode.ai/zen/v1")
TARGET_ZEN_URL = f"{TARGET_ZEN_BASE}/chat/completions"
TARGET_ZEN_ANTHROPIC_URL = f"{TARGET_ZEN_BASE}/messages"
TARGET_ZEN_RESPONSES_URL = f"{TARGET_ZEN_BASE}/responses"

MAX_RETRIES_ON_429 = int(os.environ.get("MAX_RETRIES_ON_429", "4"))
INITIAL_BACKOFF = float(os.environ.get("INITIAL_BACKOFF", "1"))
UPSTREAM_RPM = float(os.environ.get("UPSTREAM_RPM", "10"))
# 单核小机器默认 4：上游是单并发契约，20 并发只会排队 sleep + 429 + 轮换风暴
MAX_UPSTREAM_CONCURRENCY = max(1, int(os.environ.get("MAX_UPSTREAM_CONCURRENCY", "4")))
# /metrics 重型快照缓存（秒）：命中时直接复用，不再调 rotator/外部 IP 库
METRICS_CACHE_TTL_SECONDS = max(5, int(os.environ.get("METRICS_CACHE_TTL_SECONDS", "15")))
# token/history 批量刷盘：请求只写内存，后台线程定时 flush，避免每请求抢 _db_lock
METRICS_FLUSH_INTERVAL_SECONDS = max(1, int(os.environ.get("METRICS_FLUSH_INTERVAL_SECONDS", "10")))
METRICS_FLUSH_MAX_ROWS = max(10, int(os.environ.get("METRICS_FLUSH_MAX_ROWS", "200")))
REQUEST_HISTORY_RETENTION_DAYS = max(1, int(os.environ.get("REQUEST_HISTORY_RETENTION_DAYS", "7")))
REQUEST_HISTORY_MAX_ROWS = max(1000, int(os.environ.get("REQUEST_HISTORY_MAX_ROWS", "10000")))
RATE_LIMIT_ROTATION_THRESHOLD = max(1, int(os.environ.get("RATE_LIMIT_ROTATION_THRESHOLD", "2")))
MAX_RATE_LIMIT_WAIT = max(0.0, float(os.environ.get("MAX_RATE_LIMIT_WAIT", "8")))
NETWORK_FAILURE_ROTATION_THRESHOLD = max(1, int(os.environ.get("NETWORK_FAILURE_ROTATION_THRESHOLD", "2")))
WARP_ROTATOR_URL = os.environ.get("WARP_ROTATOR_URL", "http://127.0.0.1:8001").rstrip("/")
# 429 时先触发 egress 换节点（mihomo/WARP），成功则重试，而不是直接透传给客户端
ROTATE_ON_429 = os.environ.get("ROTATE_ON_429", "true").lower() in ("true", "1", "yes")
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "http://127.0.0.1:8000,http://localhost:8000").split(",")
    if origin.strip()
]

_upstream_request_semaphore = asyncio.Semaphore(MAX_UPSTREAM_CONCURRENCY)
_egress_rate_policy = EgressRatePolicy(UPSTREAM_RPM, RATE_LIMIT_ROTATION_THRESHOLD)
_egress_rotation_lock = asyncio.Lock()

metrics = {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limited_requests": 0,
    # 配额级 429（FreeUsageLimit/GoUsageLimit/BlackUsageLimit）：换 IP 无法恢复
    "quota_exhausted_requests": 0,
    "fallback_triggered": 0,
    "discovered_models_count": 0,
    "start_time": time.time()
}

DEFAULT_FREE_MODELS = [
    {"id": "mimo-v2.5-free", "name": "MiMo V2.5 Free"},
    {"id": "ling-3.0-flash-fin-free", "name": "Ling 3.0 Flash Fin Free"},
    {"id": "nemotron-3-ultra-free", "name": "Nemotron 3 Ultra Free"},
    {"id": "nemotron-3.5-lightning-free", "name": "Nemotron 3.5 Lightning Free"},
    {"id": "muse-spark-1.3-contributor-free", "name": "Muse Spark 1.3 Contributor Free"},
    {"id": "muse-spark-1.2-contributor-free", "name": "Muse Spark 1.2 Contributor Free"},
]

# 默认请求模型（可通过 DEFAULT_MODEL 覆盖）。上游 /models 返回的真实 free 模型会
# 在自动发现后整体替换 DEFAULT_FREE_MODELS；此处仅作为发现失败时的兜底默认值。
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "mimo-v2.5-free")

# 仅支持 Responses API 的模型（走 /v1/responses 而非 /v1/chat/completions）
RESPONSES_ONLY_MODELS = {
    "muse-spark-1.3-contributor-free",
    "muse-spark-1.2-contributor-free",
    "muse-spark-1.3-contributor",
    "muse-spark-1.2-contributor",
}

def _convert_messages_to_input(messages: list) -> list:
    """将 Chat Completions messages 格式转为 Responses API input 格式。
    Chat: [{"role":"user","content":"hi"}]
    Responses: [{"role":"user","content":[{"type":"input_text","text":"hi"}]}]
    """
    result = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "tool":
            output = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
            result.append({
                "type": "function_call_output",
                "call_id": msg.get("tool_call_id") or msg.get("call_id") or msg.get("id") or "call_unknown",
                "output": output,
            })
            continue
        tool_calls = msg.get("tool_calls")
        if role == "assistant" and isinstance(tool_calls, list):
            if content:
                text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
                result.append({
                    "role": role,
                    "content": [{"type": "output_text", "text": text}],
                })
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                fn = tool_call.get("function") or {}
                if not isinstance(fn, dict):
                    continue
                name = fn.get("name") or tool_call.get("name")
                if not name:
                    continue
                arguments = fn.get("arguments", "")
                if not isinstance(arguments, str):
                    arguments = json.dumps(arguments, ensure_ascii=False)
                result.append({
                    "type": "function_call",
                    "call_id": tool_call.get("id") or tool_call.get("call_id") or f"call_{uuid.uuid4().hex[:20]}",
                    "name": name,
                    "arguments": arguments,
                })
            continue
        if content is None:
            continue
        text_type = "output_text" if role == "assistant" else "input_text"
        if isinstance(content, str):
            result.append({
                "role": role,
                "content": [{"type": text_type, "text": content}]
            })
        elif isinstance(content, list):
            if role == "assistant":
                content = [
                    {**item, "type": "output_text"}
                    if isinstance(item, dict) and item.get("type") == "input_text"
                    else item
                    for item in content
                ]
            result.append({
                "role": role,
                "content": content
            })
        else:
            result.append(msg)
    return result


def _convert_tools_to_responses_format(tools: list) -> list:
    """将 Chat Completions tools 格式转为 Responses API tools 格式。
    Chat: [{"type":"function","function":{"name":"get_weather","description":"...","parameters":{...}}}]
    Responses: [{"type":"function","name":"get_weather","description":"...","parameters":{...}}]
    """
    result = []
    for tool in tools:
        if not isinstance(tool, dict):
            result.append(tool)
            continue
        t = dict(tool)
        if t.get("type") == "function" and "function" in t:
            fn = t.pop("function")
            if isinstance(fn, dict):
                t["name"] = fn.get("name", "")
                if "description" in fn:
                    t["description"] = fn["description"]
                if "parameters" in fn:
                    t["parameters"] = fn["parameters"]
                if "strict" in fn:
                    t["strict"] = fn["strict"]
        if t.get("type") == "function" and not t.get("name"):
            continue
        result.append(t)
    return result


def _convert_tool_choice_to_responses_format(tool_choice):
    """将 Chat Completions tool_choice 格式转为 Responses API 格式。
    上游仅支持字符串 "auto"，所有其他值均回退为 auto 或移除。
    """
    if isinstance(tool_choice, str):
        return "auto" if tool_choice != "none" else None
    if not isinstance(tool_choice, dict):
        return "auto"
    tc_type = tool_choice.get("type", "")
    if tc_type == "none":
        return None
    return "auto"


async def responses_stream_as_chat(response, model_name: str):
    """将 Responses API 的流式 SSE 事件转为 Chat Completions SSE 格式。"""
    loop = asyncio.get_event_loop()
    pending_sse_event = ""
    message_id = f"chatcmpl-{uuid.uuid4().hex[:20]}"
    created = int(time.time())
    reasoning_text = ""
    finish_reason = "stop"
    usage = {}
    tool_call_state = {}

    def make_chunk(delta: dict, finish: str = None) -> bytes:
        chunk = {
            "id": message_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_name,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish,
            }]
        }
        return b"data: " + json.dumps(chunk, ensure_ascii=False, separators=(",", ":")).encode() + b"\n\n"

    # 先发 role chunk
    yield make_chunk({"role": "assistant", "content": ""})

    def get_next_line(iter_lines):
        try:
            return next(iter_lines)
        except StopIteration:
            return None
        except Exception as exc:
            log.error("[responses_stream_as_chat] iter_lines error: %s: %s", type(exc).__name__, exc)
            return None

    line_iter = response.iter_lines()
    _prefetch = []
    _discard_stream_session = False

    async def _next_stream_item():
        nonlocal _prefetch
        if not _prefetch:
            _prefetch = await _fetch_line_batch(loop, get_next_line, line_iter, STREAM_FETCH_BATCH)
            if not _prefetch:
                return None
        return _prefetch.pop(0)

    try:
        while True:
            item = await _next_stream_item()
            if item is None:
                break
            if not item:
                yield b"\n"
                continue

            raw = item.decode("utf-8", errors="ignore").strip()
            if raw.startswith("event:"):
                pending_sse_event = raw[6:].strip()
                continue
            if not raw.startswith("data:"):
                continue

            pending_sse_event = ""
            try:
                data = json.loads(raw[5:])
            except json.JSONDecodeError:
                continue

            event_type = data.get("type", "")
            if event_type == "response.output_text.delta":
                delta_text = data.get("delta", "")
                yield make_chunk({"content": delta_text})
            elif event_type == "response.output_text.done":
                pass  # 内容已通过 delta 发送
            elif event_type == "response.completed":
                resp_data = data.get("response") or {}
                usage = resp_data.get("usage", {})
                status = resp_data.get("status", "completed")
                incomplete = resp_data.get("incomplete_details") or {}
                if status == "incomplete" and incomplete.get("reason") == "max_output_tokens":
                    finish_reason = "length"
            elif event_type == "response.output_item.added":
                item = data.get("item") or {}
                if item.get("type") == "function_call":
                    output_index = data.get("output_index", 0)
                    key = item.get("id") or output_index
                    tool_call_state[key] = {
                        "index": output_index,
                        "id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:20]}",
                        "name": item.get("name") or "tool",
                    }
            elif event_type == "response.function_call_arguments.delta":
                output_index = data.get("output_index", 0)
                key = data.get("item_id") or output_index
                state = tool_call_state.get(key) or tool_call_state.get(output_index)
                if not state:
                    state = {
                        "index": output_index,
                        "id": data.get("call_id") or data.get("item_id") or f"call_{uuid.uuid4().hex[:20]}",
                        "name": data.get("name") or "tool",
                    }
                    tool_call_state[key] = state
                yield make_chunk({
                    "tool_calls": [{
                        "index": state["index"],
                        "id": state["id"],
                        "type": "function",
                        "function": {
                            "name": state["name"],
                            "arguments": data.get("delta", ""),
                        },
                    }]
                })
            elif event_type == "error":
                _discard_stream_session = True
                break

        # 发 finish chunk
        yield make_chunk({}, finish_reason)
        yield b"data: [DONE]\n\n"

        # 更新 token usage
        if usage:
            track_token_usage(
                model_name,
                prompt_tokens=usage.get("input_tokens", DEFAULT_PROMPT_TOKENS),
                completion_tokens=usage.get("output_tokens", DEFAULT_COMPLETION_TOKENS),
            )
    finally:
        if session is not None:
            if stream_pool_key is not None:
                _release_stream_session(stream_pool_key, session, discard=_discard_stream_session)
            else:
                try:
                    session.close()
                except Exception:
                    pass


def _convert_responses_to_chat(resp: dict, model_name: str) -> dict:
    """将 Responses API 响应格式转为 Chat Completions 格式。
    Responses: {"object":"response","output":[{"type":"reasoning","encrypted_content":"..."},{"type":"message","content":[{"type":"output_text","text":"hi"}]}]}
    Chat: {"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi","reasoning":"..."},"finish_reason":"stop"}]}
    """
    output = resp.get("output", [])
    reasoning_text = ""
    message_text = ""
    for item in output:
        if item.get("type") == "reasoning":
            reasoning_text = item.get("encrypted_content", "") or ""
        elif item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    message_text = c.get("text", "")

    status = resp.get("status", "completed")
    incomplete = resp.get("incomplete_details") or {}
    if status == "incomplete":
        reason = incomplete.get("reason", "")
        finish_reason = "length" if reason == "max_output_tokens" else "stop"
    else:
        finish_reason = "stop"

    message = {"role": "assistant", "content": message_text}
    if reasoning_text:
        message["reasoning"] = reasoning_text

    return {
        "id": resp.get("id", ""),
        "object": "chat.completion",
        "created": resp.get("created_at", int(time.time())),
        "model": model_name,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason
        }],
        "usage": resp.get("usage", {}),
    }


discovered_models: List[Dict[str, str]] = DEFAULT_FREE_MODELS.copy()
_discovery_lock = threading.Lock()

# 按客户端标识缓存 x-opencode-session，同一客户端复用同一 session
_session_cache: Dict[str, str] = {}
_session_cache_lock = threading.Lock()

# session → proxy 绑定：不同会话使用不同代理出口
_session_proxy_map: Dict[str, str] = {}
_session_proxy_map_lock = threading.Lock()

LOG_FORMAT = os.environ.get("LOG_FORMAT", "text").lower()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = getattr(logging, LOG_LEVEL, logging.INFO)

if LOG_FORMAT == "json":
    _handler = logging.StreamHandler()
    _handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=LOG_LEVEL_VALUE, handlers=[_handler], force=True)
else:
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=LOG_LEVEL_VALUE,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
log = logging.getLogger("zen_server")

@asynccontextmanager
async def lifespan(application: FastAPI):
    global model_usage_stats
    # 先修复旧镜像写入的非法 provider header，避免 mihomo 容器因配置解析错误
    # 进入重启循环；此操作只改共享 config.yaml，不影响订阅 URL 和节点缓存。
    try:
        panel_mgr.normalize_mihomo_config()
    except Exception as exc:
        log.warning("mihomo config migration failed: %s", exc)
    init_db()
    model_usage_stats = load_metrics_from_db()
    # 出口状态由后台 watcher 异步首刷；启动路径不同步打外部站，避免拖慢 boot
    start_egress_watcher()
    start_metrics_flusher()
    _discovery_stop.clear()
    # 容器重建后 mihomo 不保留 file provider 的内存测速历史。后台等待控制器
    # 就绪并主动触发一次健康检查，使 free 节点尽快从“未检测”恢复。
    threading.Thread(
        target=panel_mgr.warmup_provider_healthchecks,
        name="mihomo-provider-warmup",
        daemon=True,
    ).start()
    threading.Thread(target=discover_models_task, daemon=True).start()
    yield
    _metrics_flush_stop.set()
    _egress_watch_stop.set()
    try:
        _flush_metrics_buffers()
    except Exception:
        pass
    _close_all_sessions()
    _discovery_stop.set()

app = FastAPI(title="OpenCode Zen V1.0 Ultra Resilient Proxy", lifespan=lifespan)

UPSTREAM_REQUEST_PATHS = {"/v1/chat/completions", "/v1/messages", "/v1/responses"}


@app.middleware("http")
async def serialize_upstream_requests(request: Request, call_next):
    """Enforce the upstream's one-request-at-a-time contract, including SSE."""
    if request.url.path not in UPSTREAM_REQUEST_PATHS:
        return await call_next(request)

    await _upstream_request_semaphore.acquire()
    try:
        response = await call_next(request)
    except Exception:
        _upstream_request_semaphore.release()
        raise

    original_iterator = response.body_iterator

    async def release_after_response():
        try:
            async for chunk in original_iterator:
                yield chunk
                # Detect client disconnect early to release semaphore
                if await request.is_disconnected():
                    log.warning("[STREAM DEBUG] Client disconnected mid-stream; releasing semaphore early.")
                    break
        finally:
            _upstream_request_semaphore.release()

    response.body_iterator = release_after_response()
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def discover_models_task():
    global discovered_models
    while not _discovery_stop.is_set():
        try:
            disc_headers = get_realistic_headers()
            req = UrlRequest(
                f"{TARGET_ZEN_BASE}/models",
                headers=disc_headers
            )
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                models_data = data.get("data", [])
                if models_data:
                    new_models = []
                    for m in models_data:
                        m_id = m.get("id", "")
                        if "free" in m_id.lower() or "zen" in m_id.lower():
                            new_models.append({"id": m_id, "name": m_id.replace("-", " ").title()})
                    
                    if new_models:
                        # 过滤掉已知不可用的模型
                        unavailable = {"deepseek-v4-flash-free", "hy3-free", "laguna-s-2.1-free"}
                        new_models = [m for m in new_models if m["id"] not in unavailable]
                        if new_models:
                            with _discovery_lock:
                                discovered_models = new_models
                                metrics["discovered_models_count"] = len(discovered_models)
                            log.info(f"Auto-Discovery refreshed: {len(discovered_models)} active model(s) fetched.")
        except Exception as e:
            log.debug(f"Auto-Discovery fallback active: {e}")
        _discovery_stop.wait(300)

class FlowContext:
    def __enter__(self):
        global active_flows_count
        with flow_lock:
            active_flows_count += 1
            prom_active_flows.set(active_flows_count)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global active_flows_count
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str = Field(default=DEFAULT_MODEL)
    messages: List[ChatMessage]
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None

def normalize_anthropic_request(body: dict) -> dict:
    """Normalize valid Anthropic shorthand content before sending it upstream.

    Anthropic clients may send ``content`` as a plain string, while Zen's
    native Messages adapter expects the canonical content-block form. Keep
    all other request fields untouched so tools, system prompts and streaming
    options continue to pass through unchanged.
    """
    normalized = dict(body or {})
    requested_max_tokens = normalized.get("max_tokens")
    if not isinstance(requested_max_tokens, int) or requested_max_tokens < ANTHROPIC_MIN_MAX_TOKENS:
        normalized["max_tokens"] = max(ANTHROPIC_MIN_MAX_TOKENS, 1024 if requested_max_tokens is None else 0)
    messages = normalized.get("messages")
    if isinstance(messages, list):
        normalized_messages = []
        for message in messages:
            if not isinstance(message, dict):
                normalized_messages.append(message)
                continue
            item = dict(message)
            content = item.get("content")
            if isinstance(content, str):
                item["content"] = [{"type": "text", "text": content}]
            normalized_messages.append(item)
        normalized["messages"] = normalized_messages

    system = normalized.get("system")
    if isinstance(system, str):
        normalized["system"] = [{"type": "text", "text": system}]
    return normalized

def _random_opencode_id(prefix: str, descending: bool = True) -> str:
    """生成 OpenCode CLI 格式的 ID，如 msg_xxx / ses_xxx
    格式：前缀 + 12位hex时间戳 + 14位随机base62 = 30字符
    与 opencode 源码 packages/schema/src/identifier.ts 一致"""
    import time
    import secrets
    timestamp_ms = int(time.time() * 1000)
    current = (timestamp_ms * 0x1000) + 0
    value = ~current if descending else current
    time_hex = format(value & 0xffffffffffff, '012x')
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    rand = ''.join(secrets.choice(chars) for _ in range(14))
    return f"{prefix}_{time_hex}{rand}"



def get_realistic_headers(client_key: str = "") -> Dict[str, str]:
    if client_key:
        with _session_cache_lock:
            if client_key not in _session_cache:
                _session_cache[client_key] = f"dsh-opencode-go-session-{uuid.uuid4().hex[:12]}"
            session = _session_cache[client_key]
    else:
        session = os.environ.get("OPENCODE_SESSION", "dsh-opencode-go-session")
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer public",
        "Accept": "application/json, text/event-stream, */*",
        "User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14",
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": _random_opencode_id("msg", descending=False),
        "x-opencode-session": _random_opencode_id("ses", descending=True),
    }


SAFE_UPSTREAM_HEADERS = {
    "content-type",
    "retry-after",
    "x-request-id",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "cf-ray",
}
SENSITIVE_LOG_KEYS = {"authorization", "api_key", "apikey", "token", "password", "secret"}


def redact_for_log(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]"
            if any(marker in key.lower().replace("-", "_") for marker in SENSITIVE_LOG_KEYS)
            else redact_for_log(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    if isinstance(value, str) and len(value) > 1000:
        return value[:1000] + "...[truncated]"
    return value


def log_upstream_response(response, model_name: str, endpoint: str, attempt: int, uses_proxy: bool) -> None:
    headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() in SAFE_UPSTREAM_HEADERS
    }
    log.debug(
        "Upstream response model=%s endpoint=%s attempt=%s status=%s uses_proxy=%s headers=%s",
        model_name,
        endpoint,
        attempt,
        response.status_code,
        uses_proxy,
        headers,
    )


def upstream_rate_limit_response(response, model_name: str) -> JSONResponse:
    category, retry_seconds, payload = classify_upstream_429(response)
    # 配额级 429（FreeUsageLimit 等）：换 IP 无法恢复，单独统计供面板提示
    if category == "quota":
        metrics["quota_exhausted_requests"] += 1
    headers = {"X-Rate-Limit-Reason": category}
    if retry_seconds is not None:
        headers["Retry-After"] = str(retry_seconds)
    log.warning(
        "Upstream 429 for model '%s' classified as %s (retry_after=%s); headers=%s payload=%s",
        model_name,
        category,
        retry_seconds,
        {key: value for key, value in response.headers.items() if key.lower() in SAFE_UPSTREAM_HEADERS},
        redact_for_log(payload),
    )
    return JSONResponse(status_code=429, content=payload, headers=headers)


def rotate_egress(reason: str) -> tuple[bool, Optional[str]]:
    """Request rotation from the service that owns the shared WARP namespace."""
    try:
        response = cffi_requests.post(f"{WARP_ROTATOR_URL}/rotate", timeout=ROTATE_REQUEST_TIMEOUT_SECONDS)
        data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if response.status_code == 200 and data.get("status") == "success":
            return True, data.get("verified_ip")
        return False, None
    except Exception as exc:
        log.warning("Rotator request failed: %s", exc)
        return False, None


def _egress_policy_key(proxies: Optional[Dict[str, str]]) -> str:
    proxy_url = _request_proxy_url(proxies)
    if proxy_url:
        return f"proxy:{proxy_url}"
    # 稳定键：同一 mihomo 出口复用同一 pacing 槽。原来的 updated_at 一切换就变，
    # 老槽的 next_allowed 直接作废，等于 pacing 失效后突发打上游。
    route = proxy_pool.routing_snapshot()
    mode = route.get("mode", "mihomo")
    if mode == "mihomo":
        return "mihomo:active"
    return f"{mode}:{route.get('active_proxy') or 'default'}"


async def pace_egress_request(proxies: Optional[Dict[str, str]]) -> str:
    """Wait for this egress's next evenly-spaced request slot."""
    egress_key = _egress_policy_key(proxies)
    wait_seconds = _egress_rate_policy.reserve(egress_key)
    if wait_seconds > 0:
        # 高频路径：INFO 会烧磁盘 + 拖慢单核机，只在 DEBUG 留
        log.debug("Pacing %s for %.2fs to stay within %.1f RPM.", egress_key, wait_seconds, UPSTREAM_RPM)
        await asyncio.sleep(wait_seconds)
    return egress_key


async def rotate_egress_safely(reason: str) -> tuple[bool, Optional[str]]:
    """Serialize a route switch and keep new model requests out of the transition."""
    async with _egress_rotation_lock:
        signal_rotation_start()
        try:
            return await asyncio.to_thread(rotate_egress, reason)
        finally:
            signal_rotation_done()


async def recover_from_rate_limit(
    response,
    model_name: str,
    endpoint: str,
    session,
    pooled: bool,
    egress_key: str,
    attempt: int,
    stream_pool_key=None,
) -> bool:
    """Retry a transient 429 once on the same IP before considering rotation."""
    category, retry_after, _ = classify_upstream_429(response)
    if category == "quota":
        return False

    streak, rotate_now = _egress_rate_policy.record_rate_limit(egress_key)
    if not rotate_now:
        requested_delay = retry_after if retry_after is not None else 0.0
        delay = min(requested_delay, MAX_RATE_LIMIT_WAIT)
        _reset_request_session(endpoint, session, pooled=pooled, stream_pool_key=stream_pool_key)
        log.warning(
            "HTTP 429 for '%s' on %s (streak=%d, retry_after=%s); retaining the egress and retrying in %.2fs.",
            model_name,
            egress_key,
            streak,
            requested_delay,
            delay,
        )
        if delay:
            await asyncio.sleep(delay)
        return True

    if not ROTATE_ON_429 or attempt >= MAX_RETRIES_ON_429:
        return False

    rotated, new_ip = await rotate_egress_safely(
        f"confirmed HTTP 429 x{streak} (attempt {attempt}/{MAX_RETRIES_ON_429})"
    )
    if not rotated:
        log.warning("Confirmed HTTP 429 for '%s', but egress rotation failed.", model_name)
        return False

    _reset_request_session(endpoint, session, pooled=pooled, stream_pool_key=stream_pool_key)
    log.warning("Confirmed HTTP 429 for '%s'; egress rotated -> %s. Retrying.", model_name, new_ip)
    return True


def is_egress_transport_error(exc: Exception) -> bool:
    """Distinguish a broken route from local/session or upstream application errors."""
    message = str(exc).lower()
    if "session is closed" in message:
        return False
    markers = (
        "timeout", "timed out", "connection refused", "connection reset", "connection aborted",
        "failed to connect", "could not connect", "proxy", "sock", "eof", "network is unreachable",
    )
    return any(marker in message for marker in markers)

class EmptyStreamError(Exception):
    """Raised when upstream returns an empty or truncated stream without valid content/tool calls."""
    pass

def _parse_stream_event(raw_line: bytes) -> tuple[str, Optional[dict], str]:
    """Parse one SSE data line for Responses and Anthropic compatibility."""
    decoded = raw_line.decode("utf-8", errors="ignore").strip()
    if not decoded or decoded.startswith(":"):
        return "", None, ""
    if decoded.startswith("event:"):
        return decoded[6:].strip(), None, ""
    data_text = decoded[5:].strip() if decoded.startswith("data:") else decoded
    if data_text == "[DONE]":
        return "__done__", None, ""
    try:
        payload = json.loads(data_text)
    except (TypeError, ValueError):
        return "", None, ""
    if not isinstance(payload, dict):
        return "", None, ""

    event_type = str(payload.get("type") or "")
    delta = payload.get("delta")
    if isinstance(delta, dict):
        # Anthropic native stream: {delta: {type: text_delta, text: ...}}
        delta = delta.get("text") if isinstance(delta.get("text"), str) else ""
    elif not isinstance(delta, str):
        delta = payload.get("text") if isinstance(payload.get("text"), str) else ""

    # Some upstream compatibility paths return Chat Completions chunks even
    # when the client called /v1/responses or /v1/messages.
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0] if isinstance(choices[0], dict) else {}
        choice_delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
        choice_message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
        candidate = choice_delta.get("content") or choice_message.get("content")
        if isinstance(candidate, str):
            delta = candidate
            event_type = "__chat_content__"
        elif event_type == "":
            event_type = "__chat_chunk__"

    return event_type, payload, delta


def _responses_completed_event(model_name: str, latest_response: Optional[dict], output_text: str) -> bytes:
    """Return a minimal valid Responses API response.completed SSE event."""
    completed_response = dict(latest_response or {})
    completed_response.setdefault("id", f"resp-zen-{uuid.uuid4().hex[:20]}")
    completed_response.setdefault("object", "response")
    completed_response.setdefault("model", model_name)
    completed_response["status"] = "completed"
    completed_response.setdefault("output", [])
    if output_text and not completed_response["output"]:
        completed_response["output"] = [{
            "type": "message",
            "id": f"msg-zen-{uuid.uuid4().hex[:20]}",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": output_text, "annotations": []}],
        }]
    payload = {"type": "response.completed", "response": completed_response}
    return (
        b"event: response.completed\n"
        + b"data: "
        + json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        + b"\n\n"
    )

async def stream_response(response, model_name: str, session=None, protocol: str = "chat", stream_pool_key=None) -> AsyncGenerator[bytes, None]:
    """Forward SSE and normalize terminal events for the selected API protocol."""
    loop = asyncio.get_event_loop()
    global active_flows_count
    is_responses_api = protocol == "responses"
    is_anthropic_api = protocol == "anthropic"

    with flow_lock:
        active_flows_count += 1
        prom_active_flows.set(active_flows_count)
    lease_id = await asyncio.to_thread(acquire_flow_lease)

    async def keep_flow_lease_alive():
        try:
            while True:
                await asyncio.sleep(FLOW_LEASE_HEARTBEAT_SECONDS)
                await asyncio.to_thread(touch_flow_lease, lease_id)
        except asyncio.CancelledError:
            return

    lease_heartbeat = asyncio.create_task(keep_flow_lease_alive())
    chunk_count = 0
    buffered_lines = []
    has_meaningful_content = False
    response_completed_seen = False
    latest_response = None
    output_text_parts = []
    terminal_error = False
    pending_sse_event = ""
    anthropic_message_id = f"msg-zen-{uuid.uuid4().hex[:20]}"
    anthropic_stop_reason = "end_turn"
    anthropic_stop_sequence = None
    anthropic_usage = {"input_tokens": 0, "output_tokens": 0}

    def inspect_line(line: bytes) -> tuple[str, Optional[dict], str]:
        nonlocal has_meaningful_content, response_completed_seen, latest_response, pending_sse_event
        nonlocal anthropic_message_id, anthropic_stop_reason, anthropic_stop_sequence, anthropic_usage
        raw = line.decode("utf-8", errors="ignore").strip()
        if is_responses_api and raw.startswith("event:"):
            pending_sse_event = raw[6:].strip()
            event_type, payload, delta = pending_sse_event, None, ""
        elif is_responses_api:
            event_type, payload, delta = _parse_stream_event(line)
            if pending_sse_event and not event_type:
                event_type = pending_sse_event
            if raw.startswith("data:"):
                pending_sse_event = ""
        elif is_anthropic_api:
            event_type, payload, delta = _parse_stream_event(line)
            if pending_sse_event and not event_type:
                event_type = pending_sse_event
            if raw.startswith("data:"):
                pending_sse_event = ""
        else:
            payload = None
            delta = ""
            event_type = "__chat_content__" if any(
                token in raw for token in ("content", "tool_calls", "reasoning_content")
            ) else ""
        if event_type == "response.completed":
            response_completed_seen = True
        if isinstance(payload, dict) and isinstance(payload.get("response"), dict):
            latest_response = payload["response"]
        if is_anthropic_api and isinstance(payload, dict):
            if isinstance(payload.get("id"), str):
                anthropic_message_id = payload["id"]
            usage = payload.get("usage")
            if isinstance(usage, dict):
                for key in ("input_tokens", "output_tokens"):
                    if isinstance(usage.get(key), int):
                        anthropic_usage[key] = usage[key]
                # OpenAI-compatible tail uses prompt/completion token names.
                if isinstance(usage.get("prompt_tokens"), int):
                    anthropic_usage["input_tokens"] = usage["prompt_tokens"]
                if isinstance(usage.get("completion_tokens"), int):
                    anthropic_usage["output_tokens"] = usage["completion_tokens"]
            event_delta = payload.get("delta")
            if isinstance(event_delta, dict):
                if isinstance(event_delta.get("stop_reason"), str):
                    anthropic_stop_reason = event_delta["stop_reason"]
                if "stop_sequence" in event_delta:
                    anthropic_stop_sequence = event_delta.get("stop_sequence")
        if delta:
            # 只有 responses 补 completion 事件才需要攒全文；chat/anthropic 直接透传
            if is_responses_api:
                output_text_parts.append(delta)
        if event_type == "__chat_content__" or event_type in {
            "response.output_text.delta",
            "response.output_text.done",
            "response.function_call_arguments.delta",
            "response.output_item.added",
            "response.output_item.done",
            "response.completed",
        } or bool(delta):
            has_meaningful_content = True
        return event_type, payload, delta

    def is_done_sentinel(line: bytes) -> bool:
        return (is_responses_api or is_anthropic_api) and _parse_stream_event(line)[0] == "__done__"

    def anthropic_event(event_type: str, payload: dict) -> bytes:
        return (
            f"event: {event_type}\n".encode("utf-8")
            + b"data: "
            + json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n\n"
        )

    def anthropic_delta_event(text: str) -> bytes:
        return anthropic_event("content_block_delta", {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        })

    try:
        if is_anthropic_api:
            yield anthropic_event("message_start", {
                "type": "message_start",
                "message": {
                    "id": anthropic_message_id,
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": model_name,
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": anthropic_usage,
                },
            })
            yield anthropic_event("content_block_start", {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            })

        def get_next_line(iter_lines):
            try:
                return next(iter_lines)
            except StopIteration:
                return "STOP_ITERATION"
            except Exception as exc:
                log.error(
                    "[STREAM DEBUG] Upstream socket/connection error for '%s': %s: %s",
                    model_name, type(exc).__name__, exc,
                )
                return "SOCKET_ERROR"

        line_iter = response.iter_lines()
        _prefetch = []

        async def _next_stream_item():
            nonlocal _prefetch
            if not _prefetch:
                _prefetch = await _fetch_line_batch(loop, get_next_line, line_iter, STREAM_FETCH_BATCH)
                if not _prefetch:
                    return "STOP_ITERATION"
            return _prefetch.pop(0)

        while len(buffered_lines) < 10:
            item = await _next_stream_item()
            if item in ("STOP_ITERATION", "SOCKET_ERROR"):
                break
            if not item:
                buffered_lines.append(item)
                continue
            event_type, _, _ = inspect_line(item)
            if event_type == "__done__":
                break
            raw_text = item.decode("utf-8", errors="ignore").strip()
            if raw_text and not raw_text.startswith(":"):
                buffered_lines.append(item)
                if has_meaningful_content:
                    break

        if not has_meaningful_content and len(buffered_lines) < 4:
            all_buffered = "".join(line.decode("utf-8", errors="ignore") for line in buffered_lines if line)
            empty_chat = not is_responses_api and not is_anthropic_api and (
                '"choices":[]' in all_buffered.replace(" ", "") or not buffered_lines
            )
            empty_responses = is_responses_api and not buffered_lines
            empty_anthropic = is_anthropic_api and not buffered_lines
            if empty_chat or empty_responses or empty_anthropic:
                raise EmptyStreamError("Upstream returned empty response stream")

        for line in buffered_lines:
            if not line:
                chunk_count += 1
                yield b"\n"
                continue
            if is_done_sentinel(line):
                continue
            chunk_count += 1
            if is_anthropic_api:
                _, _, delta = inspect_line(line)
                if delta:
                    yield anthropic_delta_event(delta)
            else:
                yield line + b"\n"

        while True:
            item = await _next_stream_item()
            if item == "STOP_ITERATION":
                break
            if item == "SOCKET_ERROR":
                terminal_error = True
                break
            if not item:
                chunk_count += 1
                yield b"\n"
                continue
            _, _, delta = inspect_line(item)
            if is_done_sentinel(item):
                continue
            chunk_count += 1
            if is_anthropic_api:
                if delta:
                    yield anthropic_delta_event(delta)
            else:
                yield item + b"\n"

        if is_responses_api and not response_completed_seen and not terminal_error:
            if not has_meaningful_content:
                raise EmptyStreamError("Upstream returned a Responses stream without output")
            yield _responses_completed_event(model_name, latest_response, "".join(output_text_parts))
            log.warning(
                "[RESPONSES COMPAT] Upstream ended without response.completed for '%s'; synthesized completion event.",
                model_name,
            )
        elif is_anthropic_api:
            if not terminal_error:
                yield anthropic_event("content_block_stop", {
                    "type": "content_block_stop", "index": 0,
                })
                yield anthropic_event("message_delta", {
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": anthropic_stop_reason,
                        "stop_sequence": anthropic_stop_sequence,
                    },
                    "usage": {"output_tokens": anthropic_usage.get("output_tokens", 0)},
                })
                yield anthropic_event("message_stop", {"type": "message_stop"})
        else:
            yield b"\ndata: [DONE]\n\n"

        log.info("Streaming completed successfully for '%s' (%s lines sent).", model_name, chunk_count)
    except EmptyStreamError:
        _discard_stream_session = True
        raise
    except GeneratorExit:
        _discard_stream_session = True
        log.warning(
            "[STREAM DEBUG] Client explicitly closed/aborted SSE for '%s' after %s lines.",
            model_name, chunk_count,
        )
    except Exception as exc:
        log.error("Stream exception for '%s': %s: %s", model_name, type(exc).__name__, exc, exc_info=True)
        if is_anthropic_api:
            yield b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"stream_error\",\"message\":\"Upstream stream interrupted\"}}\n\n"
        elif not is_responses_api:
            yield b"\ndata: [DONE]\n\n"
    finally:
        lease_heartbeat.cancel()
        await asyncio.gather(lease_heartbeat, return_exceptions=True)
        await asyncio.to_thread(release_flow_lease, lease_id)
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)
        if session is not None:
            if stream_pool_key is not None:
                _release_stream_session(stream_pool_key, session, discard=_discard_stream_session or terminal_error)
            else:
                try:
                    session.close()
                except Exception:
                    pass
# 根路径直达控制面板（无需 /dashboard 后缀）
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return _templates.TemplateResponse(request=request, name="dashboard.html")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return _templates.TemplateResponse(request=request, name="dashboard.html")

@app.get("/metrics-prometheus")
async def metrics_prometheus():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/rotate")
async def manual_rotate():
    started = time.monotonic()
    result, verified_ip = await rotate_egress_safely("Manual API trigger")
    record_warp_rotation(result, (time.monotonic() - started) * 1000, new_ip=verified_ip or "")
    if result:
        swap_warp_registration()
        return {"status": "success", "verified_ip": verified_ip, **proxy_pool.routing_snapshot()}
    raise HTTPException(status_code=503, detail="WARP rotator did not complete the requested rotation")

# =============================================================================
# 面板管理 API（订阅/节点/组/延迟测试）
# =============================================================================
class AddSubscriptionBody(BaseModel):
    url: str

class RemoveSubscriptionBody(BaseModel):
    name: str

class SetNodesBody(BaseModel):
    enabled: List[str] = []

class SaveGroupBody(BaseModel):
    name: str
    nodes: List[str] = []

class ApplyGroupBody(BaseModel):
    name: str

class TestDelayBody(BaseModel):
    nodes: Optional[List[str]] = None
    mode: str = Field(default="batch", pattern="^(batch|selected)$")
    background: bool = False


class DeleteNodesBody(BaseModel):
    nodes: List[str] = Field(default_factory=list)


class CleanupFailedNodesBody(BaseModel):
    failure_threshold: Optional[int] = Field(default=None, ge=1, le=100)


class FetchFreeNodesBody(BaseModel):
    region_mode: str = Field(default="auto", pattern="^(auto|manual)$")
    region: Optional[str] = None
    max_nodes: Optional[int] = Field(default=None, ge=1, le=2000)


class AddProxiesBody(BaseModel):
    text: str

class CheckProxiesBody(BaseModel):
    addrs: Optional[List[str]] = None

class RemoveProxyBody(BaseModel):
    addr: str

@app.get("/api/panel/subscriptions")
async def panel_subscriptions():
    return JSONResponse(await asyncio.to_thread(panel_mgr.list_subscriptions))

@app.post("/api/panel/subscriptions")
async def panel_add_subscription(body: AddSubscriptionBody):
    result = await asyncio.to_thread(panel_mgr.add_subscription, body.url)
    # “配置已保存但上游/热重载失败”不能伪装成“添加失败”，否则用户刷新页面
    # 后会发现订阅其实已经存在，造成重复点击和重复创建。
    if not result.get("ok") or not result.get("created"):
        raise HTTPException(status_code=400, detail=result.get("error", "添加失败"))
    return result

@app.delete("/api/panel/subscriptions/{name}")
async def panel_remove_subscription(name: str):
    result = await asyncio.to_thread(panel_mgr.remove_subscription, name)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "删除失败"))
    return result

@app.post("/api/panel/refresh")
async def panel_refresh_subscriptions():
    result = await asyncio.to_thread(panel_mgr.refresh_subscriptions)
    failures = "；".join(f"{item.get('name')}: {item.get('error')}" for item in result.get("results", []) if not item.get("ok"))
    # 只要配置文件写入成功，就返回 200，让前端能够刷新列表并显示部分失败详情；
    # 不再因为单个订阅或一次热重载失败而把整个操作显示成“刷新失败”。
    return {
        **result,
        "ok": bool(result.get("ok")),
        "partial": bool(failures) or not result.get("reload_ok", False),
        "detail": failures or ("订阅缓存已更新，但 mihomo 热重载未确认" if not result.get("reload_ok", False) else f"已刷新 {result.get('ok_count', 0)}/{result.get('total', 0)} 个订阅"),
    }

@app.get("/api/panel/nodes")
async def panel_nodes():
    return JSONResponse(await asyncio.to_thread(panel_mgr.list_nodes))

@app.put("/api/panel/nodes")
async def panel_set_nodes(body: SetNodesBody):
    result = await asyncio.to_thread(panel_mgr.set_enabled_nodes, body.enabled)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "设置失败"))
    return result

@app.post("/api/panel/nodes/clear-active-group")
async def panel_clear_active_group():
    result = await asyncio.to_thread(panel_mgr.clear_active_group)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "退出应用组失败"))
    return result


@app.get("/api/panel/groups")
async def panel_groups():
    return JSONResponse(await asyncio.to_thread(panel_mgr.list_groups))

@app.post("/api/panel/groups")
async def panel_save_group(body: SaveGroupBody):
    result = await asyncio.to_thread(panel_mgr.save_group, body.name, body.nodes)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "保存组失败"))
    return result


@app.post("/api/panel/groups/extract-healthy")
async def panel_extract_healthy_group():
    # 500+ 节点的独立测速可能持续数分钟，改为后台任务，避免浏览器/反向
    # 代理因长连接超时而把“任务仍在执行”误显示成失败。
    return await asyncio.to_thread(panel_mgr.start_extract_healthy_group_job)


@app.get("/api/panel/groups/extract-healthy/{job_id}")
async def panel_extract_healthy_group_status(job_id: str):
    result = await asyncio.to_thread(panel_mgr.get_extract_healthy_group_job, job_id)
    if not result.get("ok"):
        return JSONResponse(status_code=404, content=result)
    return result


@app.delete("/api/panel/groups/{name}")
async def panel_delete_group(name: str):
    result = await asyncio.to_thread(panel_mgr.delete_group, name)
    return result

@app.post("/api/panel/groups/apply")
async def panel_apply_group(body: ApplyGroupBody):
    result = await asyncio.to_thread(panel_mgr.apply_group, body.name)
    if not result.get("ok"):
        if result.get("code") == "group_nodes_missing":
            # 409 表示组仍存在，但其快照依赖的订阅节点已经不完整；
            # 前端据此询问用户是否删除该组，而不是把它当成普通 404。
            return JSONResponse(status_code=409, content=result)
        raise HTTPException(status_code=404, detail=result.get("error", "应用组失败"))
    return result

@app.post("/api/panel/test-delay")
async def panel_test_delay(body: TestDelayBody):
    names = body.nodes or []
    # 大批量探测改走后台任务；小规模选中测速仍保持原有同步返回格式。
    if body.background or len(names) > panel_mgr.LATENCY_BATCH_SIZE:
        result = await asyncio.to_thread(panel_mgr.start_node_delay_job, names)
    else:
        result = await asyncio.to_thread(panel_mgr.test_nodes_delay, body.nodes, body.mode)
    return JSONResponse(result, status_code=200 if result.get("ok", True) else 422)


@app.get("/api/panel/test-delay/{job_id}")
async def panel_test_delay_status(job_id: str):
    result = await asyncio.to_thread(panel_mgr.get_node_delay_job, job_id)
    if not result.get("ok"):
        return JSONResponse(status_code=404, content=result)
    return result


@app.post("/api/panel/current-node-delay")
async def panel_current_node_delay():
    result = await asyncio.to_thread(panel_mgr.test_current_node_delay)
    return JSONResponse(result, status_code=200 if result.get("ok") else 503)


@app.post("/api/panel/nodes/delete")
async def panel_delete_nodes(body: DeleteNodesBody):
    result = await asyncio.to_thread(panel_mgr.remove_selected_nodes, body.nodes)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "删除失败"))
    return result


@app.post("/api/panel/nodes/cleanup-failed")
async def panel_cleanup_failed_nodes(body: CleanupFailedNodesBody):
    threshold = body.failure_threshold or panel_mgr.NODE_FAILURE_THRESHOLD
    result = await asyncio.to_thread(panel_mgr.cleanup_failed_nodes, threshold)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "清理失败"))
    return result

# --- 代理池管理（手动出站代理列表） ---
@app.get("/api/panel/egress")
async def panel_egress_status():
    """Show which egress currently handles model traffic."""
    return JSONResponse(await asyncio.to_thread(proxy_pool.egress_status, _proxy_pool))

@app.get("/api/panel/proxies")
async def panel_proxies():
    return JSONResponse(await asyncio.to_thread(panel_mgr.list_proxies))

@app.post("/api/panel/proxies")
async def panel_add_proxies(body: AddProxiesBody):
    result = await asyncio.to_thread(panel_mgr.add_proxies, body.text)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail="代理写入失败")
    load_proxy_list()
    return result

@app.delete("/api/panel/proxies/{addr:path}")
async def panel_remove_proxy(addr: str):
    result = await asyncio.to_thread(panel_mgr.remove_proxy, addr)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "删除失败"))
    load_proxy_list()
    return result

@app.get("/api/panel/history")
def api_request_history(limit: int = 100):
    """Return recent request history records."""
    try:
        rows = _db_fetchall(
            "SELECT id, timestamp, model, proxy_ip, egress_ip, latency_ms, status, is_stream, attempt, error_msg "
            "FROM request_history ORDER BY id DESC LIMIT ?",
            (min(limit, 500),),
        )
        result = []
        for row in rows:
            if hasattr(row, 'keys'):
                result.append(dict(row))
            else:
                result.append({
                    "id": row[0], "timestamp": row[1], "model": row[2],
                    "proxy_ip": row[3], "egress_ip": row[4], "latency_ms": row[5],
                    "status": row[6], "is_stream": row[7], "attempt": row[8], "error_msg": row[9],
                })
        return {"ok": True, "history": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/panel/proxies/check")
async def panel_check_proxies(body: CheckProxiesBody):
    result = await asyncio.to_thread(panel_mgr.check_proxies, body.addrs)
    return JSONResponse(result)

# --- 免费节点模块（无订阅用户一键启用） ---
@app.get("/api/panel/free-nodes")
async def panel_free_nodes_status():
    return JSONResponse(await asyncio.to_thread(panel_mgr.get_free_nodes_status))

@app.get("/api/panel/free-nodes/regions")
async def panel_free_nodes_regions():
    return JSONResponse(await asyncio.to_thread(panel_mgr.get_free_node_region_options))


@app.post("/api/panel/free-nodes/fetch")
async def panel_fetch_free_nodes(body: FetchFreeNodesBody):
    max_nodes = body.max_nodes or panel_mgr.FREE_NODES_MAX
    result = await asyncio.to_thread(panel_mgr.fetch_free_nodes, max_nodes, body.region_mode, body.region)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "抓取失败"))
    return result

@app.get("/metrics")
async def get_metrics():
    """Cached snapshot: heavy rebuild (DB + rotator + external IP) at most every TTL."""
    global _metrics_snapshot, _metrics_snapshot_at
    now = time.monotonic()
    with _metrics_snapshot_lock:
        if _metrics_snapshot is not None and (now - _metrics_snapshot_at) < METRICS_CACHE_TTL_SECONDS:
            snapshot = dict(_metrics_snapshot)
            snapshot["uptime_seconds"] = int(time.time() - metrics["start_time"])
            snapshot["active_flows"] = active_flows_count
            snapshot["metrics"] = metrics
            snapshot["model_usage"] = model_usage_stats
            snapshot["rotation_in_progress"] = _rotation_in_progress.is_set()
            return snapshot
    fresh = await _build_metrics_snapshot()
    with _metrics_snapshot_lock:
        _metrics_snapshot = fresh
        _metrics_snapshot_at = time.monotonic()
    return fresh


async def _build_metrics_snapshot():
    uptime = int(time.time() - metrics["start_time"])

    # 内存统计已实时聚合；DB 合并只在快照重建时做一次对齐（重启恢复场景）
    db_usage = load_metrics_from_db()
    for m_name, m_data in db_usage.items():
        if m_name not in model_usage_stats:
            model_usage_stats[m_name] = m_data
        else:
            # Sync highest values or keep memory in sync with DB
            model_usage_stats[m_name]["requests"] = max(model_usage_stats[m_name]["requests"], m_data["requests"])
            model_usage_stats[m_name]["prompt_tokens"] = max(model_usage_stats[m_name]["prompt_tokens"], m_data["prompt_tokens"])
            model_usage_stats[m_name]["completion_tokens"] = max(model_usage_stats[m_name]["completion_tokens"], m_data["completion_tokens"])
            model_usage_stats[m_name]["total_tokens"] = max(model_usage_stats[m_name]["total_tokens"], m_data["total_tokens"])
            model_usage_stats[m_name]["estimated_cost_usd"] = max(model_usage_stats[m_name]["estimated_cost_usd"], m_data["estimated_cost_usd"])

    # Fetch live data from warp-rotator microservice (offloaded to executor — blocking call)
    rotator_ip = None
    rotator_location = None
    rotator_rotations = rotation_count
    rotator_history = []

    def fetch_rotator_status():
        try:
            r = cffi_requests.get(f"{WARP_ROTATOR_URL}/status", impersonate="chrome124", timeout=4)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"warp-rotator status fetch error: {e}")
        return None

    loop = asyncio.get_event_loop()
    rdata = await loop.run_in_executor(None, fetch_rotator_status)

    if rdata:
        rotator_ip = rdata.get("current_ip")
        rotator_rotations = rdata.get("rotations", rotation_count)
        rotator_history = rdata.get("history", [])
        if rotator_ip:
            def fetch_location():
                return get_ip_location(rotator_ip)
            rotator_location = await loop.run_in_executor(None, fetch_location)

    # Fallback: local IP lookup
    if not rotator_ip:
        def fetch_local_ip():
            ip = get_public_ip()
            loc = get_ip_location(ip) if ip else {"country": "Unknown", "flag": "🌐"}
            return ip, loc
        rotator_ip, rotator_location = await loop.run_in_executor(None, fetch_local_ip)

    # Fallback: SQLite history
    if not rotator_history:
        rotator_history = load_ip_history_from_db()

    if not rotator_location:
        rotator_location = {"country": "Unknown", "flag": "🌐"}

    return {
        "uptime_seconds": uptime,
        "verified_public_ip": rotator_ip,
        "egress_verification_scope": "shared proxy and WARP network namespace",
        "location": rotator_location,
        "total_rotations": rotator_rotations,
        "metrics": metrics,
        "active_flows": active_flows_count,
        "discovered_models": discovered_models,
        "model_usage": model_usage_stats,
        "ip_history": rotator_history,
        "warp_quality": dict(warp_quality_stats),
        "dual_warp": dict(_dual_warp),
        "rotation_in_progress": _rotation_in_progress.is_set()
    }

@app.get("/health")
async def health():
    """轻量健康检查：只读内存 + 后台缓存，绝不同步打外部站，docker 每 30s 调一次。"""
    state = _read_egress_cache()
    db_ok = bool(state.get("db_ok", True))
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "egress_ok": bool(state.get("ok", True)),
        "uptime_seconds": int(time.time() - metrics["start_time"]),
        "active_flows": active_flows_count,
        "total_rotations": rotation_count,
        "warp_quality": dict(warp_quality_stats),
    }


@app.get("/ready")
async def ready():
    """就绪探针：DB 可用即 ready；出口状态只看后台缓存，不阻塞。"""
    state = _read_egress_cache()
    db_ok = bool(state.get("db_ok", True))
    if not db_ok:
        raise HTTPException(status_code=503, detail="database unreachable")
    return {
        "status": "ready",
        "egress_ok": bool(state.get("ok", True)),
        "uptime_seconds": int(time.time() - metrics["start_time"]),
        "active_flows": active_flows_count,
    }

@app.get("/v1/models")
async def list_models():
    with _discovery_lock:
        return {
            "object": "list",
            "data": [
                {
                    "id": m["id"],
                    "object": "model",
                    "created": 1700000000,
                    "owned_by": "opencode"
                }
                for m in discovered_models
            ]
        }

@app.post("/v1/chat/completions")
async def chat_completions(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="chat", endpoint="chat_completions").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        payload = await raw_request.json()
    except Exception:
        payload = {}

    current_model = payload.get("model", DEFAULT_MODEL)
    is_stream = payload.get("stream", False)
    log.info(f"Received request for model '{current_model}' (Stream: {is_stream} | Has Tools: {'tools' in payload})")

    # Responses-only 模型：转换 body 格式并切换目标 URL
    _target_url = TARGET_ZEN_URL
    if current_model in RESPONSES_ONLY_MODELS:
        log.info(f"Model '{current_model}' is responses-only; converting to Responses API format.")
        if "messages" in payload:
            payload["input"] = _convert_messages_to_input(payload.pop("messages"))
        if "max_tokens" in payload:
            payload["max_output_tokens"] = payload.pop("max_tokens")
        # 转换 tools 格式（Chat Completions → Responses API）
        if "tools" in payload:
            payload["tools"] = _convert_tools_to_responses_format(payload["tools"])
        # 转换 tool_choice 格式（Chat Completions → Responses API）
        if "tool_choice" in payload:
            converted_tc = _convert_tool_choice_to_responses_format(payload["tool_choice"])
            if converted_tc is None:
                payload.pop("tool_choice", None)
            else:
                payload["tool_choice"] = converted_tc
        # 移除 Responses API 不支持的 Chat Completions 参数
        for key in ("reasoning_effort", "n", "frequency_penalty", "presence_penalty", "stop", "user"):
            payload.pop(key, None)
        _target_url = TARGET_ZEN_RESPONSES_URL

    client_key = raw_request.headers.get("x-api-key", "") or raw_request.headers.get("authorization", "")
    headers = get_realistic_headers(client_key)
    for k, v in raw_request.headers.items():
        if k.lower().startswith("x-opencode-"):
            headers[k] = v

    # 会话标识：优先用客户端传入的 x-opencode-session，否则用 client_key
    session_key = headers.get("x-opencode-session", "") or client_key

    consecutive_timeouts = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key)
            egress_key = await pace_egress_request(proxies)
            session = create_fresh_session(is_stream) if is_stream else _get_session("chat")
            response = session.post(
                _target_url,
                json=payload,
                headers=headers,
                impersonate="chrome124",
                stream=is_stream,
                proxies=proxies,
                timeout=MODEL_TIMEOUT_OVERRIDES.get(current_model, STREAM_TIMEOUT) if is_stream else 120
            )
            log_upstream_response(response, current_model, "chat_completions", attempt, proxies is not None)
            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=current_model).inc()
                category, _, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    return upstream_rate_limit_response(response, current_model)
                if await recover_from_rate_limit(response, current_model, "chat", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None):
                    stream_borrowed = False
                    continue
                if stream_borrowed:
                    _release_stream_session(stream_pool_key, session, discard=True)
                    stream_borrowed = False
                return upstream_rate_limit_response(response, current_model)

            if response.status_code >= 500:
                _reset_request_session("chat", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
                stream_borrowed = False
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, current_model, delay)
                await asyncio.sleep(delay)
                continue

            # 处理 400/403 等客户端错误——上游拒绝了请求，不应标记成功
            if response.status_code >= 400:
                log.warning("Upstream HTTP %s for '%s'; returning error to client.", response.status_code, current_model)
                if is_stream:
                    # 流式请求：读取错误响应体，返回 SSE 格式的错误
                    try:
                        error_lines = [line.decode("utf-8", errors="ignore").strip() for line in response.iter_lines() if line]
                        error_body = "".join(error_lines) if error_lines else "Unknown upstream error"
                    except Exception:
                        error_body = "Unknown upstream error"
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    log.warning("Upstream error body for '%s': %s", current_model, error_body[:500])
                    error_chunk = {
                        "id": f"chatcmpl-{int(time.time())}",
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": current_model,
                        "choices": [{"index": 0, "delta": {"role": "assistant", "content": f"[Upstream {response.status_code}] {error_body}"}, "finish_reason": "stop"}]
                    }
                    error_sse = f"data: {json.dumps(error_chunk, ensure_ascii=False, separators=(',', ':'))}\n\n"
                    return StreamingResponse(
                        [error_sse, "data: [DONE]\n\n"],
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
                    )
                else:
                    # 非流式请求：直接返回上游的错误响应
                    try:
                        error_body = response.json()
                    except Exception:
                        error_body = {"error": {"message": response.text[:500] if response.text else "Unknown upstream error", "type": "upstream_error"}}
                    log.warning("Upstream error body for '%s': %s", current_model, str(error_body)[:500])
                    return JSONResponse(
                        content={
                            "id": f"chatcmpl-{int(time.time())}",
                            "object": "chat.completion",
                            "created": int(time.time()),
                            "model": current_model,
                            "choices": [{"index": 0, "message": {"role": "assistant", "content": f"[Upstream {response.status_code}] {json.dumps(error_body, ensure_ascii=False)}"}, "finish_reason": "stop"}]
                        },
                        status_code=200
                    )

            if current_model in RESPONSES_ONLY_MODELS:
                log.info(f"Upstream response for '{current_model}': status={response.status_code}")
            _mark_request_proxy_success(proxies)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=current_model).inc()
            prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)

            if is_stream:
                try:
                    # Pre-verify that the response is not an empty stream before committing to StreamingResponse
                    if current_model in RESPONSES_ONLY_MODELS:
                        stream_gen = responses_stream_as_chat(response, current_model, session=session if stream_borrowed else None, stream_pool_key=stream_pool_key if stream_borrowed else None)
                    else:
                        stream_gen = stream_response(response, current_model, session=session, stream_pool_key=stream_pool_key if stream_borrowed else None)
                    # 流式只记一条 history（播完才算成功）；token 先按估算记一次，
                    # 真实 usage 由 responses 转换函数在 completion 事件里再记（幂等聚合）
                    track_token_usage(current_model, prompt_tokens=100, completion_tokens=150)
                    return StreamingResponse(
                        stream_gen,
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
                    )
                except EmptyStreamError:
                    log.warning("Empty stream for '%s'; retrying without egress rotation (%s/%s).", current_model, attempt, MAX_RETRIES_ON_429)
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    await asyncio.sleep(delay)
                    continue
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(response.json)
                        # Responses-only 模型：将 Responses API 响应转为 Chat Completions 格式
                        if current_model in RESPONSES_ONLY_MODELS:
                            res_json = _convert_responses_to_chat(res_json, current_model)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            current_model,
                            prompt_tokens=usage.get("prompt_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("completion_tokens", DEFAULT_COMPLETION_TOKENS)
                        )
                        record_request_history(
                            model=current_model,
                            proxy_ip=_request_proxy_url(proxies) or "",
                            egress_ip=egress_key,
                            latency_ms=round((time.time() - start_time) * 1000, 1),
                            status="ok",
                            is_stream=False,
                            attempt=attempt,
                        )
                        return JSONResponse(content=res_json)
                    except EmptyStreamError:
                        if stream_borrowed:
                            _release_stream_session(stream_pool_key, session, discard=True)
                            stream_borrowed = False
                        log.warning("Empty forced stream for '%s'; retrying (%s/%s).", current_model, attempt, MAX_RETRIES_ON_429)
                        delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                        await asyncio.sleep(delay)
                        continue
                    except Exception:
                        track_token_usage(current_model, prompt_tokens=DEFAULT_PROMPT_TOKENS, completion_tokens=DEFAULT_COMPLETION_TOKENS)
                        return {
                            "id": f"chatcmpl-zen-resp-{int(time.time())}",
                            "object": "chat.completion",
                            "created": int(time.time()),
                            "model": current_model,
                            "choices": [{"index": 0, "message": {"role": "assistant", "content": response.text}, "finish_reason": "stop"}]
                        }

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Connection error for model '{current_model}': {type(e).__name__}: {e}")
            transport_error = is_egress_transport_error(e)
            if transport_error:
                _mark_request_proxy_failure(proxies)
            # 代理失败时清除 session 绑定，下次请求重新选代理
            if session_key:
                _session_proxy_pop(session_key)
            _reset_request_session("chat", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
            consecutive_timeouts = consecutive_timeouts + 1 if transport_error else 0
            if consecutive_timeouts >= NETWORK_FAILURE_ROTATION_THRESHOLD and ROTATE_ON_429:
                rotated, new_ip = await rotate_egress_safely(f"Consecutive transport failures x{consecutive_timeouts} (attempt {attempt})")
                consecutive_timeouts = 0
                if rotated:
                    log.warning("Consecutive timeouts for '%s'; egress rotated -> %s. Retrying.", current_model, new_ip)
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                    continue
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for model '{current_model}'. Returning 503.")
    record_request_history(
        model=current_model,
        proxy_ip=_request_proxy_url(proxies) if proxies else "",
        egress_ip=egress_key,
        latency_ms=round((time.time() - start_time) * 1000, 1),
        status="failed",
        is_stream=is_stream,
        attempt=MAX_RETRIES_ON_429,
        error_msg="All attempts exhausted",
    )
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# -----------------------------------------------------------------------------
# Anthropic API Compatibility Endpoint (/v1/messages)
# -----------------------------------------------------------------------------
@app.post("/v1/messages")
async def anthropic_messages(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="messages", endpoint="anthropic_messages").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except Exception:
        body = {}

    body = normalize_anthropic_request(body)
    model_name = body.get("model", DEFAULT_MODEL)
    is_stream = body.get("stream", False)
    log.info(f"Received Anthropic-format request for model '{model_name}' (Stream: {is_stream})")

    client_api_key = raw_request.headers.get("x-api-key") or ""
    if not client_api_key:
        auth = raw_request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            client_api_key = auth[7:]

    headers = get_realistic_headers(client_api_key)
    headers["x-api-key"] = client_api_key or "public"

    for k, v in raw_request.headers.items():
        kl = k.lower()
        if kl.startswith("x-opencode-") or kl.startswith("anthropic-"):
            headers[k] = v

    # 会话标识：优先用客户端传入的 x-opencode-session
    session_key = headers.get("x-opencode-session", "") or client_api_key

    consecutive_timeouts = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key)
            egress_key = await pace_egress_request(proxies)
            if is_stream:
                session, stream_pool_key, _ = _get_stream_session("anthropic", egress_key)
                stream_borrowed = True
                response = await _post_upstream_stream(
                    session, TARGET_ZEN_ANTHROPIC_URL, json_body=body, headers=headers,
                    proxies=proxies,
                    timeout=MODEL_TIMEOUT_OVERRIDES.get(model_name, STREAM_TIMEOUT),
                )
            else:
                session = _get_session("anthropic")
                response = await _post_upstream(
                    session, TARGET_ZEN_ANTHROPIC_URL, json_body=body, headers=headers,
                    proxies=proxies, timeout=120,
                )
            log_upstream_response(response, model_name, "messages", attempt, proxies is not None)

            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                category, _, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    return upstream_rate_limit_response(response, model_name)
                if await recover_from_rate_limit(response, model_name, "anthropic", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None):
                    stream_borrowed = False
                    continue
                if stream_borrowed:
                    _release_stream_session(stream_pool_key, session, discard=True)
                    stream_borrowed = False
                return upstream_rate_limit_response(response, model_name)

            if response.status_code >= 500:
                _reset_request_session("anthropic", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
                stream_borrowed = False
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, model_name, delay)
                await asyncio.sleep(delay)
                continue

            _mark_request_proxy_success(proxies)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="anthropic_messages").observe(time.time() - start_time)

            if is_stream:
                return StreamingResponse(
                    stream_response(response, model_name, session=session, protocol="anthropic", stream_pool_key=stream_pool_key if stream_borrowed else None),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(response.json)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            model_name,
                            prompt_tokens=usage.get("input_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("output_tokens", DEFAULT_COMPLETION_TOKENS),
                        )
                        return JSONResponse(content=res_json)
                    except Exception:
                        track_token_usage(model_name, prompt_tokens=DEFAULT_PROMPT_TOKENS, completion_tokens=DEFAULT_COMPLETION_TOKENS)
                        return JSONResponse(content=response.text)

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Anthropic endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            transport_error = is_egress_transport_error(e)
            if transport_error:
                _mark_request_proxy_failure(proxies)
            # 代理失败时清除 session 绑定，下次请求重新选代理
            if session_key:
                _session_proxy_pop(session_key)
            _reset_request_session("anthropic", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
            consecutive_timeouts = consecutive_timeouts + 1 if transport_error else 0
            if consecutive_timeouts >= NETWORK_FAILURE_ROTATION_THRESHOLD and ROTATE_ON_429:
                rotated, new_ip = await rotate_egress_safely(f"Consecutive transport failures x{consecutive_timeouts} (attempt {attempt})")
                consecutive_timeouts = 0
                if rotated:
                    log.warning("Consecutive timeouts for '%s'; egress rotated -> %s. Retrying.", model_name, new_ip)
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                    continue
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Anthropic model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

@app.post("/v1/responses")
async def responses_endpoint(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="responses", endpoint="responses").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except Exception:
        body = {}

    model_name = body.get("model", DEFAULT_MODEL)
    is_stream = body.get("stream", False)
    log.info(f"Received Responses API request for model '{model_name}' (Stream: {is_stream})")

    client_key = raw_request.headers.get("x-api-key", "") or raw_request.headers.get("authorization", "")
    headers = get_realistic_headers(client_key)
    for k, v in raw_request.headers.items():
        if k.lower().startswith("x-opencode-"):
            headers[k] = v

    # 会话标识：优先用客户端传入的 x-opencode-session
    session_key = headers.get("x-opencode-session", "") or client_key

    consecutive_timeouts = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key)
            egress_key = await pace_egress_request(proxies)
            session = create_fresh_session(is_stream) if is_stream else _get_session("responses")
            response = session.post(
                TARGET_ZEN_RESPONSES_URL,
                json=body,
                headers=headers,
                impersonate="chrome124",
                stream=is_stream,
                proxies=proxies,
                timeout=MODEL_TIMEOUT_OVERRIDES.get(model_name, STREAM_TIMEOUT) if is_stream else 120,
            )
            log_upstream_response(response, model_name, "responses", attempt, proxies is not None)
            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                category, _, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    return upstream_rate_limit_response(response, model_name)
                if await recover_from_rate_limit(response, model_name, "responses", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None):
                    stream_borrowed = False
                    continue
                if stream_borrowed:
                    _release_stream_session(stream_pool_key, session, discard=True)
                    stream_borrowed = False
                return upstream_rate_limit_response(response, model_name)

            if response.status_code >= 500:
                _reset_request_session("responses", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
                stream_borrowed = False
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, model_name, delay)
                await asyncio.sleep(delay)
                continue

            _mark_request_proxy_success(proxies)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="responses").observe(time.time() - start_time)

            if is_stream:
                return StreamingResponse(
                    stream_response(response, model_name, session=session, protocol="responses", stream_pool_key=stream_pool_key if stream_borrowed else None),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(response.json)
                        return JSONResponse(content=res_json)
                    except Exception:
                        return JSONResponse(content=response.text)

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Responses endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            transport_error = is_egress_transport_error(e)
            if transport_error:
                _mark_request_proxy_failure(proxies)
            # 代理失败时清除 session 绑定，下次请求重新选代理
            if session_key:
                _session_proxy_pop(session_key)
            _reset_request_session("responses", session, pooled=not is_stream, stream_pool_key=stream_pool_key if stream_borrowed else None)
            consecutive_timeouts = consecutive_timeouts + 1 if transport_error else 0
            if consecutive_timeouts >= NETWORK_FAILURE_ROTATION_THRESHOLD and ROTATE_ON_429:
                rotated, new_ip = await rotate_egress_safely(f"Consecutive transport failures x{consecutive_timeouts} (attempt {attempt})")
                consecutive_timeouts = 0
                if rotated:
                    log.warning("Consecutive timeouts for '%s'; egress rotated -> %s. Retrying.", model_name, new_ip)
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                    continue
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Responses model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# Global exception handler for standard error format
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error(f"Unhandled error on {request.method} {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": {"message": "Internal server error", "type": "internal_error", "code": 500}},
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": exc.detail, "type": "http_error", "code": exc.status_code}},
    )

if __name__ == "__main__":
    load_proxy_list()
    log.info(f"Starting OpenCode IP Proxy Server on {HOST}:{PORT}...")
    _uvicorn_kwargs = {
        "host": HOST,
        "port": PORT,
        "timeout_keep_alive": int(os.environ.get("UVICORN_KEEPALIVE_TIMEOUT", "30")),
        "limit_concurrency": max(MAX_UPSTREAM_CONCURRENCY * 4, int(os.environ.get("UVICORN_LIMIT_CONCURRENCY", "32"))),
        "limit_max_requests": int(os.environ.get("UVICORN_LIMIT_MAX_REQUESTS", "0")) or None,
    }
    try:
        import uvloop  # noqa: F401
        _uvicorn_kwargs["loop"] = os.environ.get("UVICORN_LOOP", "uvloop")
    except Exception:
        pass
    try:
        import httptools  # noqa: F401
        _uvicorn_kwargs["http"] = os.environ.get("UVICORN_HTTP", "httptools")
    except Exception:
        pass
    uvicorn.run(app, **{k: v for k, v in _uvicorn_kwargs.items() if v is not None})
