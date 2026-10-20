import asyncio
import contextvars
import json
import logging
import os
import random
import re
import signal
import sqlite3
import threading
import time
import secrets
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Tuple
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
# 流式上游总超时（秒）。原为硬编码 60，慢模型长生成会被中途截断；改为可配置，
# 默认 120 与下面非流式分支的 timeout=120 对齐，消除两路超时不一致。
STREAM_TIMEOUT = max(10, int(os.environ.get("STREAM_TIMEOUT", "120")))

# Per-model timeout overrides (seconds) for known slow models
MODEL_TIMEOUT_OVERRIDES = {
    "hy3-free": 30,  # hy3-free is very slow (~90s), cut it short
}
# Reasoning-heavy free models can consume a tiny Anthropic max_tokens budget
# before emitting visible text. Keep the compatibility endpoint usable for
# short connection tests while allowing deployments to override the floor.
ANTHROPIC_MIN_MAX_TOKENS = max(0, int(os.environ.get("ANTHROPIC_MIN_MAX_TOKENS", "128")))
FLOW_LEASE_TTL_SECONDS = int(os.environ.get("FLOW_LEASE_TTL_SECONDS", "90"))
FLOW_LEASE_HEARTBEAT_SECONDS = int(os.environ.get("FLOW_LEASE_HEARTBEAT_SECONDS", "30"))
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

def get_next_outbound_proxy(session_key: str = "", model: Optional[str] = None) -> Optional[Dict[str, str]]:
    """Return the current unified egress.

    A manual rotation can temporarily hold the route on mihomo while it walks
    through ROTATOR nodes. In that state do not immediately re-select a custom
    proxy on the next model request.

    If session_key is provided, the proxy is cached per-session so that all
    requests in the same conversation share the same egress.
    New sessions are assigned to idle (unbound) proxies when available.
    When model is provided, proxies whose upstream quota for that model is
    still cooling down are skipped (with fallback to unfiltered selection so
    a fully-blocked pool degrades to previous behavior instead of outage).
    """
    custom_proxy = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()
    route = proxy_pool.routing_snapshot()

    # session 绑定：同一会话复用同一代理
    if session_key:
        bound_proxy = _session_proxy_get(session_key)
        if bound_proxy:
            # 检查绑定的代理是否仍可用（含该模型的配额冷却）
            if proxy_pool.is_proxy_eligible(bound_proxy, model=model):
                if UPSTREAM_API_KEY_OVERRIDE.lower() == "public":
                    # 匿名按会话绑定：活跃会话稳定复用；长间隔后视为新会话，
                    # 换池内下一个出口（分摊各 IP 日配额）
                    now = time.monotonic()
                    with _anon_egress_lock:
                        if len(_anon_last_seen) >= SESSION_CACHE_MAX_KEYS and session_key not in _anon_last_seen:
                            # O(1) 淘汰一个而非全清，避免满时所有会话同时重绑造成抖动
                            try:
                                _anon_last_seen.pop(next(iter(_anon_last_seen)), None)
                            except StopIteration:
                                pass
                        last = _anon_last_seen.get(session_key, 0.0)
                        _anon_last_seen[session_key] = now
                    if last and now - last > ANON_SESSION_GAP_SECONDS:
                        nxt = None
                        try:
                            nxt = proxy_pool.next_proxy_after(bound_proxy, model=model)
                        except Exception as exc:
                            log.warning("Anonymous new-session egress advance failed: %s", exc)
                        if nxt and nxt != bound_proxy:
                            _session_proxy_set(session_key, nxt)
                            log.info("New anonymous session after %.0fs gap; egress %s -> %s.",
                                     now - last, bound_proxy, nxt)
                            return {"http": nxt, "https": nxt}
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
        all_eligible = proxy_pool.eligible_proxies(model=model)
        if not all_eligible and model:
            # 该模型配额全满时退化为不过滤，保证不断流（行为与原来一致）
            all_eligible = proxy_pool.eligible_proxies()
            log.debug(f"Session {session_key[:20]}... all proxies quota-blocked for '{model}'; falling back to unfiltered.")
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
# 2026-10-15 埋点：单请求级 trace（TTFT / 输出量 / finish_reason / 错误类型 …），
# 用于事后分析性能与质量问题。同样走批量刷盘，热路径零 I/O。
_pending_trace_rows: deque = deque(maxlen=5000)
TRACE_MAX_ROWS = max(10000, int(os.environ.get("TRACE_MAX_ROWS", "200000")))
TRACE_ENABLED = os.environ.get("TRACE_ENABLED", "true").lower() in ("true", "1", "yes")
_metrics_flush_stop = threading.Event()
_metrics_flush_thread_started = False


_prune_counter = 0
_prune_counter_lock = threading.Lock()


def _prune_request_history(force: bool = False) -> None:
    # 每 6 次 flush（约 60s）才做一次重 DELETE，避免每 10s 全表扫描；
    # 强制模式供启动/手动调用。
    global _prune_counter
    if not force:
        with _prune_counter_lock:
            _prune_counter += 1
            if _prune_counter % 6 != 0:
                return
    try:
        cutoff = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time() - REQUEST_HISTORY_RETENTION_DAYS * 86400),
        )
        _db_execute(
            "DELETE FROM request_history WHERE created_at < ? OR timestamp < ?",
            (cutoff, cutoff),
        )
        # 用 id 上界删除替代 NOT IN SELECT 全表排序：O(logN) 而非 O(N logN)
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    row = conn.execute("SELECT MAX(id) FROM request_history").fetchone()
                finally:
                    conn.close()
            max_id = int(row[0]) if row and row[0] is not None else 0
            if max_id > REQUEST_HISTORY_MAX_ROWS:
                _db_execute(
                    "DELETE FROM request_history WHERE id <= ?",
                    (max_id - REQUEST_HISTORY_MAX_ROWS,),
                )
        except Exception:
            _db_execute(
                "DELETE FROM request_history WHERE id NOT IN "
                "(SELECT id FROM request_history ORDER BY id DESC LIMIT ?)",
                (REQUEST_HISTORY_MAX_ROWS,),
            )
    except Exception:
        pass


def _prune_request_trace() -> None:
    """按条数上界滚动清理 trace，避免小机磁盘被埋点撑爆。"""
    try:
        with _db_lock:
            conn = _get_conn()
            try:
                row = conn.execute("SELECT MAX(id) FROM request_trace").fetchone()
            finally:
                conn.close()
        max_id = int(row[0]) if row and row[0] is not None else 0
        if max_id > TRACE_MAX_ROWS:
            _db_execute(
                "DELETE FROM request_trace WHERE id <= ?",
                (max_id - TRACE_MAX_ROWS,),
            )
    except Exception:
        try:
            _db_execute(
                "DELETE FROM request_trace WHERE id NOT IN "
                "(SELECT id FROM request_trace ORDER BY id DESC LIMIT ?)",
                (TRACE_MAX_ROWS,),
            )
        except Exception:
            pass


def _flush_metrics_buffers() -> None:
    usage_batch = []
    history_batch = []
    trace_batch = []
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
    while _pending_trace_rows and len(trace_batch) < METRICS_FLUSH_MAX_ROWS:
        try:
            trace_batch.append(_pending_trace_rows.popleft())
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
                   (timestamp, model, proxy_ip, egress_ip, latency_ms, status, is_stream,
                    attempt, error_msg, client)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                history_batch,
            )
        except Exception:
            pass
        _prune_request_history()
    if trace_batch:
        try:
            _db_executemany(
                """INSERT INTO request_trace
                   (ts, ts_ms, model, endpoint, client, is_stream, client_tools,
                    proxy_ip, egress_ip,
                    ttft_ms, total_ms, chunks, out_bytes, status, attempt, finish_reason,
                    downstream_tool_calls, upstream_status, error_type,
                    prompt_tokens, completion_tokens)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                trace_batch,
            )
        except Exception:
            pass
        _prune_request_trace()
    # 全局计数器随批量刷盘一起落盘（约每 10s），重建容器后可恢复，总请求数不再归零
    try:
        save_global_counters()
    except Exception:
        pass


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
            client TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 2026-10-15 埋点表：单请求级性能/质量明细。
    # ttft_ms 用于区分「prefill 慢」与「生成慢」；finish_reason / downstream_tool_calls
    # 用于发现「下游收到无法执行的工具调用 → 卡死」这类质量问题。
    _db_execute("""
        CREATE TABLE IF NOT EXISTS request_trace (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            ts_ms INTEGER,
            model TEXT,
            endpoint TEXT,
            client TEXT,
            is_stream INTEGER DEFAULT 0,
            client_tools INTEGER DEFAULT 0,
            proxy_ip TEXT,
            egress_ip TEXT,
            ttft_ms REAL,
            total_ms REAL,
            chunks INTEGER,
            out_bytes INTEGER,
            status TEXT,
            attempt INTEGER DEFAULT 1,
            finish_reason TEXT,
            downstream_tool_calls INTEGER DEFAULT 0,
            upstream_status INTEGER,
            error_type TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER
        )
    """)
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_request_trace_ts_ms "
        "ON request_trace(ts_ms)"
    )
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_request_trace_model "
        "ON request_trace(model)"
    )
    # 2026-10-15：客户端标识。SQLite 不能给已有表加列，需按需 ALTER 补齐。
    for _tbl, _coldef in (
        ("request_history", "client TEXT"),
        ("request_trace", "client TEXT"),
    ):
        try:
            _cols = {r[1] for r in _db_fetchall("PRAGMA table_info(%s)" % _tbl)}
            if _cols and _coldef.split()[0] not in _cols:
                _db_execute("ALTER TABLE %s ADD COLUMN %s" % (_tbl, _coldef))
                log.info("Migrated %s: added column '%s'.", _tbl, _coldef.split()[0])
        except Exception as _e:
            log.warning("Column migration failed for %s.%s: %s",
                        _tbl, _coldef.split()[0], _e)
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_request_history_created "
        "ON request_history(created_at)"
    )
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_request_history_timestamp "
        "ON request_history(timestamp)"
    )
    _db_execute(
        "CREATE INDEX IF NOT EXISTS idx_active_flow_leases_expires "
        "ON active_flow_leases(expires_at)"
    )
    _db_execute("""
        CREATE TABLE IF NOT EXISTS global_counters (
            key TEXT PRIMARY KEY,
            value INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


PERSISTED_COUNTER_KEYS = (
    "total_requests",
    "successful_requests",
    "rate_limited_requests",
    "quota_exhausted_requests",
    "fallback_triggered",
)


def load_global_counters() -> Dict[str, int]:
    if not DB_FILE.exists():
        return {}
    try:
        rows = _db_fetchall("SELECT key, value FROM global_counters")
        return {r[0]: int(r[1] or 0) for r in rows}
    except Exception as e:
        log.error(f"Error loading global counters: {e}")
        return {}


def save_global_counters() -> None:
    try:
        rows = [(k, int(metrics.get(k, 0) or 0)) for k in PERSISTED_COUNTER_KEYS]
        _db_executemany("""
            INSERT INTO global_counters (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
        """, rows)
    except Exception as e:
        log.error(f"Failed to save global counters: {e}")


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
    client: str = "",
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
                client[:32] if client else "",
            )
        )
    except Exception:
        pass


def classify_trace_error(status_code=None, error_msg: str = "", exc=None) -> str:
    """把失败归成可统计的类别，便于按类型看趋势（而不是一堆各异的字符串）。"""
    msg = (error_msg or "")
    low = msg.lower()
    exc_name = type(exc).__name__.lower() if exc is not None else ""
    if status_code == 429 or "429" in low or "rate_limit" in low or "rate limit" in low:
        return "rate_limit"
    if status_code == 403 or "403" in low or "freetier" in low:
        return "forbidden"
    if "empty" in low and "stream" in low:
        return "empty_stream"
    if "exhausted" in low or status_code == 503:
        return "exhausted"
    if "client_closed" in low or isinstance(exc, GeneratorExit):
        return "client_closed"
    if "timeout" in low or "timedout" in low or "timeout" in exc_name:
        return "timeout"
    if "socket" in low or "connection" in low or "transport" in low:
        return "network"
    if status_code and 500 <= status_code < 600:
        return "upstream_5xx"
    if status_code and 400 <= status_code < 500:
        return "upstream_4xx"
    return "other"


def record_request_trace(
    *,
    model: str = "",
    endpoint: str = "",
    client: str = "",
    is_stream: bool = False,
    client_tools: bool = False,
    proxy_ip: str = "",
    egress_ip: str = "",
    ttft_ms=None,
    total_ms: float = 0.0,
    chunks: int = 0,
    out_bytes: int = 0,
    status: str = "ok",
    attempt: int = 1,
    finish_reason: str = "",
    downstream_tool_calls: bool = False,
    upstream_status=None,
    error_type: str = "",
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
):
    """埋点：单请求级性能/质量明细。入队即可，后台线程批量落库。"""
    if not TRACE_ENABLED:
        return
    try:
        now = time.time()
        _pending_trace_rows.append((
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
            int(now * 1000),
            model or "",
            endpoint or "",
            client[:32] if client else "",
            1 if is_stream else 0,
            1 if client_tools else 0,
            proxy_ip or "",
            egress_ip or "",
            round(ttft_ms, 1) if ttft_ms is not None else None,
            round(total_ms, 1),
            int(chunks or 0),
            int(out_bytes or 0),
            status or "ok",
            int(attempt or 1),
            finish_reason or "",
            1 if downstream_tool_calls else 0,
            upstream_status,
            error_type or "",
            int(prompt_tokens or 0),
            int(completion_tokens or 0),
        ))
    except Exception:
        pass


async def _stream_with_history(inner_gen, *, model: str, proxies, egress_key: str, start_time: float,
                               attempt: int, endpoint: str = "chat", client_tools: bool = False,
                               upstream_status=None, client: str = ""):
    """Wrap SSE generator so streaming requests also leave a request_history row.

    StreamingResponse consumes the generator after the endpoint returns, so the
    history row must be written inside the generator's finally block, otherwise
    /api/panel/history never sees stream=true requests.

    2026-10-15 埋点：顺带采集 TTFT、输出量、finish_reason、下游是否收到 tool_calls。
    只在含 finish_reason / tool_calls 的少量 chunk 上做 JSON 解析，热路径开销可忽略。
    """
    status = "ok"
    error_msg = ""
    first_ts = None
    nbytes = 0
    nchunks = 0
    finish_reason = ""
    saw_tool_calls = False
    try:
        async for chunk in inner_gen:
            if chunk:
                nbytes += len(chunk)
                nchunks += 1
                if first_ts is None and chunk.strip():
                    first_ts = time.time()
                if b'"finish_reason"' in chunk or b'"tool_calls"' in chunk:
                    try:
                        _s = chunk.decode("utf-8", "ignore").strip()
                        if _s.startswith("data:"):
                            _d = json.loads(_s[5:].strip())
                            for _c in (_d.get("choices") or []):
                                if not isinstance(_c, dict):
                                    continue
                                if _c.get("finish_reason"):
                                    finish_reason = str(_c["finish_reason"])
                                _dc = _c.get("delta")
                                if isinstance(_dc, dict) and _dc.get("tool_calls"):
                                    saw_tool_calls = True
                    except Exception:
                        pass
            yield chunk
    except GeneratorExit:
        status = "failed"
        error_msg = "client_closed"
        raise
    except Exception as exc:
        status = "failed"
        error_msg = f"{type(exc).__name__}: {exc}"[:200]
        raise
    finally:
        _total_ms = round((time.time() - start_time) * 1000, 1)
        try:
            record_request_history(
                model=model,
                proxy_ip=_request_proxy_url(proxies) or "",
                egress_ip=egress_key or "",
                latency_ms=_total_ms,
                status=status,
                is_stream=True,
                attempt=attempt,
                error_msg=error_msg,
                client=client,
            )
        except Exception:
            pass
        try:
            record_request_trace(
                model=model,
                endpoint=endpoint,
                client=client,
                is_stream=True,
                client_tools=client_tools,
                proxy_ip=_request_proxy_url(proxies) or "",
                egress_ip=egress_key or "",
                ttft_ms=((first_ts - start_time) * 1000) if first_ts else None,
                total_ms=_total_ms,
                chunks=nchunks,
                out_bytes=nbytes,
                status=status,
                attempt=attempt,
                finish_reason=finish_reason,
                downstream_tool_calls=saw_tool_calls,
                upstream_status=upstream_status,
                error_type=(classify_trace_error(upstream_status, error_msg)
                            if status != "ok" else ""),
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
# 全局 key 上限：egress_key 可随代理/会话增长，无上限会导致内存泄漏；超限淘汰最早 key
STREAM_POOL_MAX_KEYS = max(8, int(os.environ.get("STREAM_POOL_MAX_KEYS", "32")))


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
        if pool_key not in _stream_pool and len(_stream_pool) >= STREAM_POOL_MAX_KEYS:
            try:
                _stream_pool.pop(next(iter(_stream_pool)), None)
            except StopIteration:
                pass
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
# Model pricing reference (USD per 1M tokens) — paid-tier official prices,
# used to estimate savings vs. free-tier usage.
# Sources (2026-09): Meta dev docs (muse contributor $0.10/$0.20),
# Xiaomi mimo.mi.com overseas USD (mimo-v2.5/v2.6-flash $0.14/$0.28),
# DeepInfra/NVIDIA (nemotron-3-ultra $0.50/$2.20, 3.5-lightning $0.05/$0.20),
# Poolside list (laguna-s-2.1 $0.10/$0.20), DeepSeek docs V4.1 Flash
# (deepseek-v4-flash $0.15/$0.60), Tencent Cloud docs (hy3 $0.132/$0.528),
# OpenRouter (ling-3.0-flash $0.021/$0.063).
MODEL_PRICING = {
    "deepseek-v4-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.60},
    "mimo-v2.5-free": {"input_per_1m": 0.14, "output_per_1m": 0.28},
    "mimo-v2.6-flash-free": {"input_per_1m": 0.14, "output_per_1m": 0.28},
    "nemotron-3-ultra-free": {"input_per_1m": 0.50, "output_per_1m": 2.20},
    "nemotron-3.5-lightning-free": {"input_per_1m": 0.05, "output_per_1m": 0.20},
    "laguna-s-2.1-free": {"input_per_1m": 0.10, "output_per_1m": 0.20},
    "hy3-free": {"input_per_1m": 0.132, "output_per_1m": 0.528},
    "ling-3.0-flash-fin-free": {"input_per_1m": 0.021, "output_per_1m": 0.063},
    "muse-spark-1.2-contributor-free": {"input_per_1m": 0.10, "output_per_1m": 0.20},
    "muse-spark-1.3-contributor-free": {"input_per_1m": 0.10, "output_per_1m": 0.20},
}

_model_usage_lock = threading.Lock()

# --- /metrics snapshot cache + egress status cache ---------------------------
# /metrics 原来每次都 doing DB 全表 + rotator HTTP + 外部 IP 库，被每秒抓取时就是烧 CPU。
_metrics_snapshot = None
_metrics_snapshot_at = 0.0
_metrics_snapshot_lock = threading.Lock()

# 出口状态由后台线程定期刷新（默认 60s），/health 与 /metrics 只读缓存，不再同步打外部站
_egress_cache = {"ok": True, "ip": None, "db_ok": True, "checked_at": 0.0, "rotations": None}
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
    # 轮换次数由 rotator 独立容器维护，proxy 进程内的 `rotation_count` 只是导入快照、
    # 永不更新（2026-10-14 诊断 P2）。后台顺带从 rotator /status 刷新真实值供 /health。
    rotations = None
    try:
        r = cffi_requests.get(f"{WARP_ROTATOR_URL}/status", impersonate="chrome124", timeout=4)
        if r.status_code == 200:
            rotations = r.json().get("rotations")
    except Exception:
        pass
    with _egress_cache_lock:
        update = {"ok": ok, "ip": ip, "db_ok": db_ok, "checked_at": time.time()}
        if rotations is not None:
            update["rotations"] = int(rotations)
        _egress_cache.update(update)
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

def track_token_usage(model_name: str, prompt_tokens: int = 0, completion_tokens: int = 0, count_request: bool = True):
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
        # 流式请求在开始时已用估算记过一次 requests，结束时只补真实 token，
        # 否则每个流式请求被数两次请求数（count_request=False）
        if count_request:
            model_usage_stats[model_name]["requests"] += 1
        model_usage_stats[model_name]["prompt_tokens"] += prompt_tokens
        model_usage_stats[model_name]["completion_tokens"] += completion_tokens
        model_usage_stats[model_name]["total_tokens"] += (prompt_tokens + completion_tokens)
        model_usage_stats[model_name]["estimated_cost_usd"] += cost

    # 热路径只入队，后台线程批量刷盘；内存统计已实时更新，/metrics 不依赖本条落盘
    try:
        _pending_usage_rows.append(
            (model_name, 1 if count_request else 0, prompt_tokens, completion_tokens, prompt_tokens + completion_tokens, cost)
        )
    except Exception:
        pass

def _pick_usage_num(usage, *keys, default=0):
    """从上游 usage 字典取第一个为数字的键（兼容 prompt_tokens/input_tokens 两种命名）。"""
    if not isinstance(usage, dict):
        return default
    for k in keys:
        v = usage.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            return int(v)
    return default

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
# 2026-10-15：上游对匿名（Bearer public）通道加了客户端指纹闸门，旧的 /zen/v1 对
# 匿名通道已被 console 代理剥掉凭证，恒 403 FreeTierError。匿名免费层现在只走
# /inference/openai/v1（实测：同请求形状 /zen/v1 → 403，新端点 → 200 真流式）。
TARGET_ZEN_BASE = os.environ.get("OPENCODE_ZEN_TARGET_BASE", "https://opencode.ai/inference/openai/v1")
TARGET_ZEN_URL = f"{TARGET_ZEN_BASE}/chat/completions"
TARGET_ZEN_ANTHROPIC_URL = f"{TARGET_ZEN_BASE}/messages"
TARGET_ZEN_RESPONSES_URL = f"{TARGET_ZEN_BASE}/responses"
# 模型列表在旧端点仍公开可读（无需认证），作为发现失败时的兜底。
TARGET_MODELS_BASES = [TARGET_ZEN_BASE, "https://opencode.ai/zen/v1"]

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

# 2026-10-16：凭据级限流识别。
# 实测 7 次 exhausted 全是 429，且每次出口都不同（121/123/124/125/127/129），
# 换 IP 后仍 429 —— 说明匿名通道 Bearer public 的限流是凭据级（全局共享），
# 不按出口计。继续轮换只会把健康出口拖进冷却并空转 12-14s。
#
# CREDENTIAL_LIMIT_ROTATION_BUDGET = 单次请求内最多因 429 轮换几次出口。
# 用完后再遇 429 → 判定凭据级，快速失败并把 Retry-After 透给下游自行退避。
CREDENTIAL_LIMIT_ROTATION_BUDGET = max(
    0, int(os.environ.get("CREDENTIAL_LIMIT_ROTATION_BUDGET", "1")))
# 判定为凭据级后是否跳过 per-egress 配额记录（跳过可避免误伤健康出口）
CREDENTIAL_LIMIT_SKIP_QUOTA = os.environ.get(
    "CREDENTIAL_LIMIT_SKIP_QUOTA", "true").lower() in ("true", "1", "yes")
# key 级长封禁阈值（秒）：retry-after 超过它说明不是 IP 级瞬时限流，
# 换出口也无用，直接快速失败并把 Retry-After 透给下游退避。
LONG_BAN_THRESHOLD_SECONDS = max(30, int(os.environ.get("LONG_BAN_THRESHOLD_SECONDS", "120")))
MAX_RATE_LIMIT_WAIT = max(0.0, float(os.environ.get("MAX_RATE_LIMIT_WAIT", "8")))
# 429 重试退避下限（秒）：上游未返回 Retry-After 时也不能 0s 立即重试，
# 否则突发场景会以秒级横扫整个出口池（2026-10-14 诊断 P1）。
RATE_LIMIT_MIN_BACKOFF_SECONDS = max(0.0, float(os.environ.get("RATE_LIMIT_MIN_BACKOFF_SECONDS", "1.5")))
# 确认 429 换出口后的退避（秒）：给上游与新出口一个缓冲，抑制 429 风暴。
RATE_LIMIT_ROTATE_BACKOFF_SECONDS = max(0.0, float(os.environ.get("RATE_LIMIT_ROTATE_BACKOFF_SECONDS", "1.0")))
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
    {"id": "mimo-v2.6-flash-free", "name": "MiMo V2.6 Flash Free"},
    {"id": "ling-3.0-flash-fin-free", "name": "Ling 3.0 Flash Fin Free"},
    {"id": "nemotron-3-ultra-free", "name": "Nemotron 3 Ultra Free"},
    {"id": "nemotron-3.5-lightning-free", "name": "Nemotron 3.5 Lightning Free"},
    {"id": "muse-spark-1.3-contributor-free", "name": "Muse Spark 1.3 Contributor Free"},
    {"id": "muse-spark-1.2-contributor-free", "name": "Muse Spark 1.2 Contributor Free"},
]

# 默认请求模型（可通过 DEFAULT_MODEL 覆盖）。上游 /models 返回的真实 free 模型会
# 在自动发现后整体替换 DEFAULT_FREE_MODELS；此处仅作为发现失败时的兜底默认值。
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "mimo-v2.6-flash-free")

# 仅支持 Responses API 的模型（走 /v1/responses 而非 /v1/chat/completions）
RESPONSES_ONLY_MODELS = {
    "muse-spark-1.3-contributor-free",
    "muse-spark-1.2-contributor-free",
    "muse-spark-1.3-contributor",
    "muse-spark-1.2-contributor",
}

# 上游实测不可用的免费模型（2026-10-08/09 实测）：
#  - exo-free：上游 410 ModelDeprecated，已下线；
#  - fledge-alpha-free：能力目录 deprecated:true，调则 401 not supported，已下线；
#  - jev-1.13-free：原生 System One 协议，走 chat 必 400，应走 /v1/systemone（本代理无此端点）；
#  - deepseek/hy3/laguna：历史不可用。
# space-bunny/longcat-2.5-preview/ling-3.1-flash 已验证可用（需 agent 形状流式请求，
# 见 free-tier shim），不再屏蔽：发现列表过滤 + 推理入口快速 400，
# 避免占住单并发（MAX_UPSTREAM_CONCURRENCY=1）反复重试拖死正常请求。
UNAVAILABLE_MODELS = {
    "deepseek-v4-flash-free", "hy3-free", "laguna-s-2.1-free", "jev-1.13-free",
    "exo-free", "fledge-alpha-free",
}

def unavailable_model_response(model_name: str):
    """已知不可用模型的快速拒绝响应（不占上游并发，直接 400）。"""
    return JSONResponse(
        status_code=400,
        content={"error": {
            "message": f"Model '{model_name}' is currently unavailable upstream. Use GET /v1/models for available free models.",
            "type": "invalid_request_error",
            "code": "model_unavailable",
        }},
    )

def _convert_content_part(part: dict, text_type: str):
    """Chat Completions 内容段 → Responses API input 内容段。

    text → input_text/output_text；image_url → input_image（上游不认
    image_url，原样透传会 400 invalid_request_error）；file → input_file。
    已是 Responses 形态或未知类型则原样透传/丢弃空段。
    """
    if not isinstance(part, dict):
        return None
    ptype = part.get("type")
    if ptype == "text" or (ptype in ("input_text", "output_text") and ptype != text_type):
        return {"type": text_type, "text": part.get("text", "")}
    if ptype in ("input_text", "output_text", "input_image", "input_file"):
        return part
    if ptype == "image_url":
        iu = part.get("image_url")
        url = iu.get("url") if isinstance(iu, dict) else iu
        if not isinstance(url, str) or not url:
            return None
        out = {"type": "input_image", "image_url": url}
        if isinstance(iu, dict) and iu.get("detail"):
            out["detail"] = iu["detail"]
        return out
    if ptype == "file":
        inner = part.get("file")
        if not isinstance(inner, dict):
            return None
        out = {"type": "input_file"}
        out.update(inner)
        return out
    return part


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
            text_type = "output_text" if role == "assistant" else "input_text"
            converted = []
            for item in content:
                c = _convert_content_part(item, text_type)
                if c is not None:
                    converted.append(c)
            if not converted:
                continue
            result.append({
                "role": role,
                "content": converted
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


def _strictify_json_schema(schema):
    """Responses/Console 对 json_schema 要求每个 object 都带 additionalProperties:false。"""
    if isinstance(schema, dict):
        out = {k: _strictify_json_schema(v) for k, v in schema.items()}
        if out.get("type") == "object" and "additionalProperties" not in out:
            out["additionalProperties"] = False
        return out
    if isinstance(schema, list):
        return [_strictify_json_schema(v) for v in schema]
    return schema


def _response_format_to_text_format(rf):
    """Chat Completions response_format → Responses API text.format。

    OpenAI 上游（Zen/Console）不认 response_format（400 unknown parameter），
    但支持 text.format（json_object 已验证 200；json_schema 需附
    additionalProperties:false）。无法映射时返回 None 由调用方丢弃。
    """
    if not isinstance(rf, dict):
        return None
    rtype = rf.get("type")
    if rtype in ("json_object",):
        return {"format": {"type": "json_object"}}
    if rtype in ("json_schema", "json"):
        inner = rf.get("json_schema") if rtype == "json_schema" else rf
        if not isinstance(inner, dict):
            return None
        schema = inner.get("schema", rf.get("schema"))
        if not isinstance(schema, dict):
            return None
        return {"format": {
            "type": "json_schema",
            "name": inner.get("name") or rf.get("name") or "response",
            "schema": _strictify_json_schema(schema),
            "strict": bool(inner.get("strict", rf.get("strict", True))),
        }}
    return None


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


# --- free-tier agentic shim -------------------------------------------------
# 上游免费层要求请求"像真客户端"（与 opencode2api prepareAnonymousBody 同逻辑）：
#   stream=true 且含核心 agent 工具（bash, edit, glob, grep, read），否则 403 FreeTierError；
#   新模型（space-bunny/longcat/ling-3.1）只接受最小 tools 定义，多余字段会导致空流/403。
# 代理对免费模型自动补核心 tools；下游非流请求则强制走上游流式，
# 播完聚合拼回下游要的 JSON（stream collapse），保证下游契约不变。
FREE_TIER_SHIM_ENABLED = os.environ.get("FREE_TIER_SHIM_ENABLED", "true").lower() in ("true", "1", "yes")
# 临时诊断（req-diag / stream-summary 日志）：排查"发图后只回前半句"类问题用
REQ_DIAG_ENABLED = os.environ.get("REQ_DIAG", "0").lower() in ("true", "1", "yes")

# 2026-10-15：为过上游 403 指纹闸门，网关必须注入"官方向量"工具定义（真 description +
# 真 schema）。副作用是上游模型真的会用这些工具——对不带工具的普通聊天客户端，表现为
# 「回答一两句后发起 tool_call、finish_reason=tool_calls 就结束」，正文为空，客户端接
# 不住工具调用就一直卡住。实测 tool_choice="none" 被上游忽略，只能用 system 指令压制。
# 仅在「客户端本来就没带 tools」时注入，避免破坏 opencode CLI 这类真需要工具的客户端。
FREE_TIER_NO_TOOL_NUDGE_ENABLED = os.environ.get(
    "FREE_TIER_NO_TOOL_NUDGE", "true").lower() in ("true", "1", "yes")
FREE_TIER_NO_TOOL_NUDGE = (
    "You are answering through a plain chat completions API. "
    "Do NOT call, request, or emit any tool/function calls under any circumstances. "
    "The tool definitions present in this request are decorative and unavailable to you. "
    "Answer the user directly and completely in plain text, and never finish your reply "
    "with a tool invocation."
)

def is_free_tier_model(name) -> bool:
    return bool(name) and "free" in str(name).lower()


def _prepend_no_tool_nudge(container: dict, key: str = "messages") -> None:
    """在消息最前面压一条"禁止调用工具"的 system 指令。

    只在客户端原本没带 tools 时调用（此时 tools 是我们为过闸门注入的，不应被真的使用）。
    """
    if not FREE_TIER_NO_TOOL_NUDGE_ENABLED:
        return
    msgs = container.get(key)
    if isinstance(msgs, str):
        # 纯字符串输入（Responses API 常见）：转成标准消息列表再前置 system
        container[key] = [
            {"role": "system", "content": FREE_TIER_NO_TOOL_NUDGE},
            {"role": "user", "content": msgs},
        ]
        return
    if not isinstance(msgs, list):
        return
    for m in msgs:
        if isinstance(m, dict) and m.get("role") == "system":
            txt = m.get("content")
            if isinstance(txt, str):
                if "Do NOT call" not in txt:
                    m["content"] = FREE_TIER_NO_TOOL_NUDGE + "\n\n" + txt
            elif isinstance(txt, list):
                # 多模态 system（content 为 part 数组）时前置一条独立 system，避免破坏结构
                msgs.insert(0, {"role": "system", "content": FREE_TIER_NO_TOOL_NUDGE})
            return
    msgs.insert(0, {"role": "system", "content": FREE_TIER_NO_TOOL_NUDGE})


# 2026-10-15：上游会逐字节比对每个"它认识的工具"的 description 与 parameters schema，
# 合成 stub 必 403。bash 是 pre-2.x 拼写——上游认识但网关没有它的官方定义，注入即 403，
# 故改用 OpenCode 2.x 的 shell 拼写；官方定义见随包发布的 opencode_tools.json。
CORE_AGENT_TOOLS = ["shell", "edit", "glob", "grep", "read"]

# 假 tools 诱发调用后的续问 steering：下游没有真工具可执行，由代理侧合成
# 工具结果续问，引导模型直接作答（流/非流共用）
_FAKE_TOOL_CONTINUE_OUTPUT = (
    "工具执行环境不可用（缺少必要参数），请直接根据已看到的图片与上下文作答，"
    "不要再调用工具。/ Tool execution unavailable (missing arguments); answer "
    "directly from visible context without further tool calls."
)

TITLE_PROMPT_PREFIX = "Generate a title for this conversation:"
# 新版标题提示词（system 角色）：固定指令头，可做特征识别
TITLE_GEN_MARKERS = ("title generator", "thread title")


def _part_text(p) -> str:
    """抽取内容段文本，兼容 OpenAI / Anthropic / Responses 多种段形状。"""
    if isinstance(p, str):
        return p
    if not isinstance(p, dict):
        return ""
    for key in ("text", "content", "input", "output"):
        v = p.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _msg_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(t for t in (_part_text(p) for p in content) if t)
    if isinstance(content, dict):
        return _msg_text(content.get("content", ""))
    return ""


def _pick_title_line(candidates: list[str]) -> Optional[str]:
    """从候选文本取标题首行；跳过提示词样板本身，避免把指令当标题返回。"""
    for cand in candidates:
        for ln in cand.splitlines():
            s = ln.strip()
            if not s:
                continue
            low = s.lower()
            if s.startswith(TITLE_PROMPT_PREFIX):
                continue
            if "title generator" in low and "thread title" in low:
                continue
            if low.startswith("generate a brief title"):
                continue
            return s[:60] if len(s) <= 60 else s[:57] + "..."
    return None


def _extract_local_title(messages) -> Optional[str]:
    """Detect OpenCode title-generation requests; synthesize title locally.

    两种已知格式（均为无工具后台调用，易触发上游免费层 FreeTierError 403，
    而正常聊天带 tools 能过；标题只是装饰文本，本地合成直接返回）：
    1. 老格式：messages[0] 为 user 且以 "Generate a title for this conversation:" 开头；
    2. 新格式：首条 system 含 "title generator"+"thread title" 指令（Only thread title /
       Generate a brief title…），标题源取其后的首条 user 消息。
    Returns the title string, or None if this is not a title request.
    """
    try:
        if not isinstance(messages, list) or not messages:
            return None
        texts = [(_m.get("role", ""), _msg_text(_m.get("content", ""))) if isinstance(_m, dict) else ("", "") for _m in messages]
        first_role, first_text = texts[0]
        candidates: list[str] = []
        if first_role == "user" and first_text.startswith(TITLE_PROMPT_PREFIX):
            rest = first_text[len(TITLE_PROMPT_PREFIX):].strip()
            if rest:
                candidates.append(rest)
            candidates.extend(t for r, t in texts[1:] if t.strip())
        elif first_role in ("system", "user") and all(mk in first_text.lower() for mk in TITLE_GEN_MARKERS):
            # 新格式：首条即标题指令，其后所有 user 消息都作为标题源候选
            #（指令与正文之间可能隔着空壳 user 段，不能取一段就 break），
            # 样板行由 _pick_title_line 统一过滤。
            for r, t in texts[1:]:
                if r == "user" and t.strip():
                    candidates.append(t.strip())
            if not candidates:
                for r, t in texts:
                    if r == "user" and t.strip() and "title generator" not in t.lower():
                        candidates.append(t.strip())
                        break
        else:
            return None
        title = _pick_title_line(candidates)
        return title if title is not None else "New session"
    except Exception:
        return None


# --- 官方向量工具定义（2026-10-15）-----------------------------------------
# 上游免费层自 2026-10-14 起会逐字节比对"它认识的每个工具"的 description 与
# parameters schema：名字对但定义是合成 stub 一律 403 FreeTierError。因此随包
# 发布从官方 CLI 抓取的 opencode_tools.json，注入与同名覆盖都用真定义。
_OFFICIAL_TOOLS: Dict[str, dict] = {}
try:
    _tools_file = Path(__file__).parent / "opencode_tools.json"
    if _tools_file.exists():
        with open(_tools_file, encoding="utf-8") as _tf:
            _OFFICIAL_TOOLS = json.load(_tf)
except Exception as _e:  # pragma: no cover - 文件缺失时退化为最小 stub
    log.warning("Failed to load opencode_tools.json: %s", _e)


def _official_tool_def(name: str):
    """返回官方定义 {description, parameters}，没有则 None。"""
    d = _OFFICIAL_TOOLS.get(name)
    if isinstance(d, dict) and d.get("description") and d.get("parameters"):
        return d
    return None


def _shim_tool_responses(name: str) -> dict:
    d = _official_tool_def(name)
    if d:
        return {
            "type": "function", "name": name,
            "description": d["description"], "parameters": d["parameters"],
        }
    return {
        "type": "function", "name": name, "description": f"Agent tool {name}",
        "parameters": {"type": "object", "properties": {}},
    }


def _shim_tool_chat(name: str) -> dict:
    d = _official_tool_def(name)
    if d:
        return {
            "type": "function",
            "function": {
                "name": name, "description": d["description"],
                "parameters": d["parameters"],
            },
        }
    return {
        "type": "function",
        "function": {
            "name": name, "description": f"Agent tool {name}",
            "parameters": {"type": "object", "properties": {}},
        },
    }


def _shim_tool_anthropic(name: str) -> dict:
    d = _official_tool_def(name)
    if d:
        return {
            "name": name, "description": d["description"], "input_schema": d["parameters"],
        }
    return {
        "name": name,
        "description": f"Agent tool {name}",
        "input_schema": {"type": "object", "properties": {}},
    }


def _resp_tool_names(tools) -> set:
    names = set()
    for t in tools or []:
        if isinstance(t, dict):
            n = t.get("name")
            if not n:
                fn = t.get("function")
                if isinstance(fn, dict):
                    n = fn.get("name")
            if n:
                names.add(n)
    return names


def _upgrade_official_tools(tools: list, protocol: str) -> bool:
    """把客户端声明的"上游认识"的工具定义就地替换成官方向量定义。

    上游逐字节比对 description + parameters schema，客户端自带的同名工具（schema
    与官方不一致）会被判为 stub 而 403；换成真定义即可通过。名字上游不认识的工具
    原样保留，以免破坏下游的函数调用。返回是否有改动。
    """
    changed = False
    for idx, item in enumerate(tools):
        if not isinstance(item, dict):
            continue
        if protocol == "chat":
            fn = item.get("function")
            name = fn.get("name") if isinstance(fn, dict) else None
        else:
            name = item.get("name")
        if not name or not _official_tool_def(name):
            continue
        if protocol == "chat":
            replacement = _shim_tool_chat(name)
        elif protocol == "responses":
            replacement = _shim_tool_responses(name)
        else:
            replacement = _shim_tool_anthropic(name)
        if item != replacement:
            tools[idx] = replacement
            changed = True
    return changed


def ensure_free_tier_tools_responses(body: dict) -> dict:
    if not FREE_TIER_SHIM_ENABLED or not isinstance(body, dict):
        return body
    if not is_free_tier_model(body.get("model")):
        return body
    tools = body.get("tools")
    client_has_tools = isinstance(tools, list) and len(tools) > 0
    tools = list(tools) if isinstance(tools, list) else []
    have = _resp_tool_names(tools)
    changed = _upgrade_official_tools(tools, "responses")
    for name in CORE_AGENT_TOOLS:
        if name not in have:
            tools.append(_shim_tool_responses(name))
            have.add(name)
            changed = True
    if changed:
        log.debug("Free-tier shim injected tools for model '%s'.", body.get("model"))
    body["tools"] = tools
    if not client_has_tools:
        # Responses API 用 input（str 或 item list）；结构不确定，失败即跳过，不影响主流程
        try:
            _prepend_no_tool_nudge(body, "input")
        except Exception:
            try:
                raw = body.get("input")
                if isinstance(raw, str):
                    body["input"] = [
                        {"role": "system", "content": FREE_TIER_NO_TOOL_NUDGE},
                        {"role": "user", "content": raw},
                    ]
            except Exception:
                pass
    return body


def ensure_free_tier_tools_chat(payload: dict) -> dict:
    if not FREE_TIER_SHIM_ENABLED or not isinstance(payload, dict):
        return payload
    if not is_free_tier_model(payload.get("model")):
        return payload
    tools = payload.get("tools")
    # 客户端自带 tools 说明它真要用工具（如 opencode CLI），此时不压制工具调用
    client_has_tools = isinstance(tools, list) and len(tools) > 0
    tools = list(tools) if isinstance(tools, list) else []
    have = _resp_tool_names(tools)
    changed = _upgrade_official_tools(tools, "chat")
    for name in CORE_AGENT_TOOLS:
        if name not in have:
            tools.append(_shim_tool_chat(name))
            have.add(name)
            changed = True
    if changed:
        log.debug("Free-tier shim injected chat tools for model '%s'.", payload.get("model"))
    payload["tools"] = tools
    if not client_has_tools:
        _prepend_no_tool_nudge(payload, "messages")
    options = payload.get("stream_options")
    if not isinstance(options, dict):
        payload["stream_options"] = {"include_usage": True}
    elif not options.get("include_usage"):
        options["include_usage"] = True
    return payload


def ensure_free_tier_tools_anthropic(body: dict) -> dict:
    if not FREE_TIER_SHIM_ENABLED or not isinstance(body, dict):
        return body
    if not is_free_tier_model(body.get("model")):
        return body
    tools = body.get("tools")
    client_has_tools = isinstance(tools, list) and len(tools) > 0
    tools = list(tools) if isinstance(tools, list) else []
    have = set()
    for t in tools:
        if isinstance(t, dict) and t.get("name"):
            have.add(t.get("name"))
    changed = _upgrade_official_tools(tools, "anthropic")
    for name in CORE_AGENT_TOOLS:
        if name not in have:
            tools.append(_shim_tool_anthropic(name))
            have.add(name)
            changed = True
    if changed:
        log.debug("Free-tier shim injected anthropic tools for model '%s'.", body.get("model"))
    body["tools"] = tools
    if not client_has_tools:
        _prepend_no_tool_nudge(body, "messages")
    return body


def _collect_responses_stream_blocking(response):
    """阻塞读完上游 responses SSE，返回 completed 的 response 对象。"""
    latest = None
    texts = []
    try:
        lines = list(response.iter_lines())
    except Exception as exc:
        raise EmptyStreamError(f"stream read failed: {exc}")
    for item in lines:
        if not item:
            continue
        try:
            raw = item.decode("utf-8", errors="ignore").strip()
        except Exception:
            continue
        if not raw.startswith("data:"):
            continue
        data_str = raw[5:].strip()
        if not data_str or data_str == "[DONE]":
            continue
        try:
            data = json.loads(data_str)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        r = data.get("response")
        if isinstance(r, dict):
            latest = r
        etype = data.get("type", "")
        if etype == "response.output_text.delta":
            d = data.get("delta")
            if isinstance(d, str):
                texts.append(d)
        if etype == "response.completed":
            if isinstance(latest, dict):
                return latest
            break
    if isinstance(latest, dict):
        return latest
    if texts:
        return {"status": "completed", "output": [{"type": "message", "content": [{"type": "output_text", "text": "".join(texts)}]}]}
    raise EmptyStreamError("Upstream returned empty response stream")


async def _collect_responses_stream(response):
    return await asyncio.to_thread(_collect_responses_stream_blocking, response)


def _collect_chat_stream_blocking(response, model_name: str):
    """阻塞读完上游 Chat Completions SSE，聚合还原为单条 JSON 响应。"""
    msg_id = None
    created = int(time.time())
    role = "assistant"
    content_parts = []
    reasoning_parts = []
    tool_calls_dict = {}
    finish_reason = "stop"
    usage = {}

    try:
        lines = list(response.iter_lines())
    except Exception as exc:
        raise EmptyStreamError(f"stream read failed: {exc}")

    for item in lines:
        if not item:
            continue
        try:
            raw = item.decode("utf-8", errors="ignore").strip()
        except Exception:
            continue
        if not raw.startswith("data:"):
            continue
        data_str = raw[5:].strip()
        if not data_str or data_str == "[DONE]":
            continue
        try:
            data = json.loads(data_str)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        if not msg_id and data.get("id"):
            msg_id = data.get("id")
        if data.get("created"):
            created = data.get("created")
        if data.get("usage"):
            usage = data.get("usage")

        choices = data.get("choices") or []
        if not choices:
            continue
        choice = choices[0]
        if choice.get("finish_reason"):
            finish_reason = choice.get("finish_reason")
        delta = choice.get("delta") or {}
        if delta.get("role"):
            role = delta.get("role")
        if delta.get("content"):
            content_parts.append(delta.get("content"))
        # thinking 模型（deepseek 型）上游流式下发字段是 reasoning_content，
        # 少数上游用 reasoning；两个都收，聚合消息也双字段下发。
        _rv = delta.get("reasoning_content") or delta.get("reasoning")
        if _rv:
            reasoning_parts.append(_rv)

        for tc in delta.get("tool_calls") or []:
            idx = tc.get("index", 0)
            if idx not in tool_calls_dict:
                tool_calls_dict[idx] = {
                    "id": tc.get("id") or f"call_{uuid.uuid4().hex[:20]}",
                    "type": tc.get("type") or "function",
                    "function": {
                        "name": tc.get("function", {}).get("name", "") if tc.get("function") else "",
                        "arguments": tc.get("function", {}).get("arguments", "") if tc.get("function") else "",
                    }
                }
            else:
                fn = tc.get("function") or {}
                if fn.get("name"):
                    tool_calls_dict[idx]["function"]["name"] = fn.get("name")
                if fn.get("arguments"):
                    tool_calls_dict[idx]["function"]["arguments"] += fn.get("arguments")

    content = "".join(content_parts) if content_parts else ""
    reasoning = "".join(reasoning_parts) if reasoning_parts else None

    message = {"role": role, "content": content}
    if reasoning:
        # 双字段下发：reasoning_content 是 DeepSeek 官方字段（严格上游回传校验
        # 依据），reasoning 是 OpenRouter 风格（兼容部分客户端读取）
        message["reasoning_content"] = reasoning
        message["reasoning"] = reasoning
    if tool_calls_dict:
        sorted_calls = [tool_calls_dict[k] for k in sorted(tool_calls_dict.keys())]
        message["tool_calls"] = sorted_calls
        if finish_reason == "stop":
            finish_reason = "tool_calls"
        # 顺手把思维文本挂到本响应的 tool_call id 上，供下一轮回传注入
        if reasoning:
            for _tc in sorted_calls:
                if _tc.get("id"):
                    _reasoning_cache_put(str(_tc["id"]), reasoning)

    if not content and not reasoning and not tool_calls_dict:
        raise EmptyStreamError("Upstream returned empty chat stream")

    res = {
        "id": msg_id or f"chatcmpl-{uuid.uuid4().hex[:20]}",
        "object": "chat.completion",
        "created": created,
        "model": model_name,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
    }
    if usage:
        res["usage"] = usage
    else:
        res["usage"] = {
            "prompt_tokens": DEFAULT_PROMPT_TOKENS,
            "completion_tokens": DEFAULT_COMPLETION_TOKENS,
            "total_tokens": DEFAULT_PROMPT_TOKENS + DEFAULT_COMPLETION_TOKENS
        }
    return res


async def _collect_chat_stream(response, model_name: str):
    return await asyncio.to_thread(_collect_chat_stream_blocking, response, model_name)


# ---------------------------------------------------------------------------
# thinking 模型 reasoning_content 会话级回传缓存
# 背景：部分上游（deepseek 型 thinking 通道）强制要求工具轮回传
# reasoning_content，否则报 400 "The reasoning_content in the thinking mode
# must be passed back to the API"。部分客户端丢弃该字段导致后续轮次被拒。
# 修法：网关在透传/聚合上游流时按 tool_call id 缓存思维文本；客户端带
# assistant(tool_calls) 消息回传但缺 reasoning_content 时自动补齐。
# ---------------------------------------------------------------------------
# 默认 TTL 24h：会话经常挂数小时后才继续，缓存过期会让严格上游重新 400；
# 容量上限仍保护内存（超量淘汰最旧 1/4）。
REASONING_PASSBACK_TTL = float(os.environ.get("REASONING_PASSBACK_TTL", "86400"))
REASONING_PASSBACK_MAX = int(os.environ.get("REASONING_PASSBACK_MAX", "2000"))
# reasoning 回传强校验的厂商特征：模型名命中即视作 thinking 严格上游
# （参考 opencode2api reasoningVendorHints，另加 glm）
REASONING_REPLAY_VENDORS = ("deepseek", "kimi", "moonshot", "mimo", "xiaomimimo", "glm")
# 缓存未命中时的占位思维内容（opencode2api 同款做法）
REASONING_TOOL_PLACEHOLDER = "tool call"
# 无工具轮的 assistant 纯思考轮缺 content 时的占位（glm 等上游要求
# 非工具轮必须有 content，见 inject_reasoning_passback ②）
EMPTY_CONTENT_PLACEHOLDER = "..."
_reasoning_passback: Dict[str, Tuple[float, str]] = {}
_reasoning_passback_lock = threading.Lock()


def _reasoning_cache_put(tool_call_id: str, reasoning: str) -> None:
    if not tool_call_id or not reasoning:
        return
    now = time.time()
    with _reasoning_passback_lock:
        _reasoning_passback[tool_call_id] = (now, reasoning)
        if len(_reasoning_passback) > REASONING_PASSBACK_MAX:
            _items = sorted(_reasoning_passback.items(), key=lambda kv: kv[1][0])
            for _k, _ in _items[: max(1, len(_items) // 4)]:
                _reasoning_passback.pop(_k, None)


def _reasoning_cache_get(tool_call_id: str) -> str:
    if not tool_call_id:
        return ""
    now = time.time()
    with _reasoning_passback_lock:
        _entry = _reasoning_passback.get(tool_call_id)
        if not _entry:
            return ""
        _ts, _text = _entry
        if now - _ts > REASONING_PASSBACK_TTL:
            _reasoning_passback.pop(tool_call_id, None)
            return ""
        return _text


def inject_reasoning_passback(payload: dict, model_name: str) -> int:
    """扫描请求里的 assistant 消息，修两种客户端历史不规范导致的 400。

    ① 带 tool_calls 的轮缺 reasoning_content（deepseek/v4.1 思考模式强校验
    "must be passed back"）：优先级为 a. 网关缓存的真实思维文本 →
    b. 客户端回传的 reasoning 字符串 → c. vendor 命中时占位符兜底。
    ② 无 tool_calls 的纯思考轮既无 content：glm 等上游要求非工具轮必须有
    content（"content is required when tool_calls is absent"），vendor 命中
    时补 content 占位符。
    返回修补的消息条数（仅日志用）。
    """
    _msgs = payload.get("messages")
    if not isinstance(_msgs, list):
        return 0
    _model = (model_name or "").lower()
    _vendor = any(_h in _model for _h in REASONING_REPLAY_VENDORS)
    _n = 0
    for _m in _msgs:
        if not isinstance(_m, dict) or _m.get("role") != "assistant":
            continue
        _tcs = _m.get("tool_calls")
        if not isinstance(_tcs, list) or not _tcs:
            # 无工具轮的 assistant 历史：glm 等上游要求非工具轮必须有
            # content，客户端"纯思考轮"（只有 reasoning、content 为空）
            # 会让整轮 400：
            #   [invalid_value] messages[N]: content is required when
            #   "tool_calls" is absent
            # vendor 命中时补占位符，与 ① 的 reasoning 兜底同思路。
            if _vendor and not _m.get("content"):
                _m["content"] = EMPTY_CONTENT_PLACEHOLDER
                _n += 1
            continue
        _existing = _m.get("reasoning_content")
        if isinstance(_existing, str) and _existing:
            continue
        _text = ""
        for _tc in _tcs:
            if isinstance(_tc, dict):
                _text = _reasoning_cache_get(str(_tc.get("id") or ""))
                if _text:
                    break
        if not _text:
            _r = _m.get("reasoning")
            if isinstance(_r, str) and _r:
                _text = _r
        if not _text and _vendor:
            # 兜底：缓存未命中（网关重启 / TTL 过期 / 客户端改写 id /
            # 部署前的历史轮次）时，严格上游仍要求该字段存在——
            # 填占位符好过整轮 400。参考 opencode2api
            # normalizeToolReasoningHistory 的同款兜底。
            _text = REASONING_TOOL_PLACEHOLDER
        if _text:
            _m["reasoning_content"] = _text
            _m.pop("reasoning", None)
            _n += 1
    return _n


def build_upstream_stream_error_sse(status_code: int, error_body: str, model: str) -> str:
    """上游出错时下发 SSE 的形态。

    error（默认）：结构化错误块 data: {"error": {...}}，客户端能识别为
    失败并重试 / 切换模型，不再把上游错误伪装成一次"正常结束"——
    旧行为让会话在错误文本上继续，越滚越歪（opencode2api issue #11 同款）。
    content：旧行为，错误文本塞进助手内容 + finish_reason=stop，
    用 STREAM_UPSTREAM_ERROR_MODE=content 可回退。
    """
    if (STREAM_UPSTREAM_ERROR_MODE or "error").lower() == "content":
        return f'data: {json.dumps({"id": f"chatcmpl-{int(time.time())}", "object": "chat.completion.chunk", "created": int(time.time()), "model": model, "choices": [{"index": 0, "delta": {"role": "assistant", "content": f"[Upstream {status_code}] {error_body}"}, "finish_reason": "stop"}]}, ensure_ascii=False, separators=(",", ":"))}\n\n'
    return f'data: {json.dumps({"error": {"message": f"[Upstream {status_code}] {error_body}", "type": "upstream_error", "code": str(status_code)}}, ensure_ascii=False, separators=(",", ":"))}\n\n'


# 流式上游出错下发形态：error=结构化错误（默认）；content=旧行为
STREAM_UPSTREAM_ERROR_MODE = os.environ.get("STREAM_UPSTREAM_ERROR_MODE", "error")


def _collect_anthropic_stream_blocking(response, model_name: str):
    """阻塞读完上游 Anthropic SSE，聚合还原为单条 JSON 响应。"""
    msg_id = None
    role = "assistant"
    content_blocks = []
    stop_reason = "end_turn"
    stop_sequence = None
    usage = {"input_tokens": 0, "output_tokens": 0}

    try:
        lines = list(response.iter_lines())
    except Exception as exc:
        raise EmptyStreamError(f"stream read failed: {exc}")

    current_event = None
    curr_block = None

    for item in lines:
        if not item:
            continue
        try:
            raw = item.decode("utf-8", errors="ignore").strip()
        except Exception:
            continue
        if raw.startswith("event:"):
            current_event = raw[6:].strip()
            continue
        if not raw.startswith("data:"):
            continue
        data_str = raw[5:].strip()
        if not data_str or data_str == "[DONE]":
            continue
        try:
            data = json.loads(data_str)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        etype = current_event or data.get("type", "")

        if etype == "message_start":
            msg = data.get("message") or {}
            msg_id = msg.get("id") or msg_id
            if msg.get("usage"):
                usage.update(msg.get("usage"))
        elif etype == "content_block_start":
            block = data.get("content_block") or {}
            curr_block = dict(block)
            content_blocks.append(curr_block)
        elif etype == "content_block_delta":
            delta = data.get("delta") or {}
            dtype = delta.get("type")
            if curr_block is not None:
                if dtype == "text_delta":
                    curr_block["text"] = curr_block.get("text", "") + delta.get("text", "")
                elif dtype == "input_json_delta":
                    curr_block["partial_json"] = curr_block.get("partial_json", "") + delta.get("partial_json", "")
        elif etype == "content_block_stop":
            if curr_block and "partial_json" in curr_block:
                try:
                    curr_block["input"] = json.loads(curr_block.pop("partial_json"))
                except Exception:
                    curr_block["input"] = {}
            curr_block = None
        elif etype == "message_delta":
            delta = data.get("delta") or {}
            if delta.get("stop_reason"):
                stop_reason = delta.get("stop_reason")
            if delta.get("stop_sequence"):
                stop_sequence = delta.get("stop_sequence")
            if data.get("usage"):
                usage.update(data.get("usage"))

    if not content_blocks:
        raise EmptyStreamError("Upstream returned empty anthropic stream")

    return {
        "id": msg_id or f"msg-{uuid.uuid4().hex[:20]}",
        "type": "message",
        "role": role,
        "content": content_blocks,
        "model": model_name,
        "stop_reason": stop_reason,
        "stop_sequence": stop_sequence,
        "usage": usage,
    }


async def _collect_anthropic_stream(response, model_name: str):
    return await asyncio.to_thread(_collect_anthropic_stream_blocking, response, model_name)


async def responses_stream_as_chat(response, model_name: str, session=None, stream_pool_key=None, followup=None):
    """将 Responses API 的流式 SSE 事件转为 Chat Completions SSE 格式。

    followup（仅下游无真工具的假 tools 场景传入）：首轮若被诱发出
    function_call，不向下游播调用块（下游执行不了），而由代理侧合成工具
    结果续问一轮后继续播最终答案，保证终止。
    """
    loop = asyncio.get_event_loop()
    pending_sse_event = ""
    message_id = f"chatcmpl-{uuid.uuid4().hex[:20]}"
    created = int(time.time())
    reasoning_text = ""
    finish_reason = "stop"
    usage = {}
    tool_call_state = {}
    done_calls = []
    saw_completed = False
    completed_ok = False
    _depth = 0
    _text_len = 0
    _text_head = ""
    # 上游 output_index 含 reasoning/message 项（工具调用可能是 2 甚至更大），
    # 下游 OpenAI 协议要求 tool_calls index 从 0 连续编号，否则 AI SDK 解析
    # 成稀疏数组、调用丢失导致客户端停住。这里按出现顺序重编号。
    _next_tool_index = 0
    _emitted_ids = set()

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
                if isinstance(delta_text, str) and delta_text:
                    _text_len += len(delta_text)
                    if len(_text_head) < 120:
                        _text_head = (_text_head + delta_text)[:120]
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
                saw_completed = True
                completed_ok = (status == "completed")
                if followup is not None and _depth == 0 and completed_ok and (done_calls or tool_call_state):
                    # 假 tools 首轮被诱发调用：代理侧合成工具结果续问一轮，
                    # 不把调用抛给下游（下游无真工具可执行）
                    _f_calls = [c for c in done_calls
                                if isinstance(c, dict) and c.get("type") == "function_call"]
                    if not _f_calls:
                        # done 事件缺失兜底：用 added/delta 建的状态合成调用
                        for _st in tool_call_state.values():
                            _f_calls.append({"type": "function_call",
                                             "call_id": _st.get("id"),
                                             "name": _st.get("name") or "tool",
                                             "arguments": "{}"})
                    if _f_calls:
                        log.info("Fake-tool calls %s on '%s' (stream); continuing proxy-side (1 round).",
                                 [c.get("name") for c in _f_calls], model_name)
                        _f_payload = {
                            "model": followup.get("model", model_name),
                            "stream": True,
                            "input": list(followup.get("input") or []) + _f_calls + [
                                {"type": "function_call_output",
                                 "call_id": c.get("call_id") or c.get("id") or "call_unknown",
                                 "output": _FAKE_TOOL_CONTINUE_OUTPUT}
                                for c in _f_calls],
                        }
                        if followup.get("tools") is not None:
                            _f_payload["tools"] = followup["tools"]
                        _r2 = None
                        try:
                            _r2 = await _post_upstream_stream(
                                session, followup["url"], json_body=_f_payload,
                                headers=followup.get("headers") or {},
                                proxies=followup.get("proxies"),
                                timeout=followup.get("timeout", STREAM_TIMEOUT))
                        except Exception as exc:
                            log.warning("Stream follow-up POST failed for '%s': %s", model_name, exc)
                        if _r2 is not None and getattr(_r2, "status_code", 500) < 400:
                            try:
                                response.close()
                            except Exception:
                                pass
                            response = _r2
                            line_iter = response.iter_lines()
                            _prefetch = []
                            tool_call_state = {}
                            done_calls = []
                            usage = {}
                            finish_reason = "stop"
                            saw_completed = False
                            completed_ok = False
                            _depth = 1
                            _text_len = 0
                            _text_head = ""
                            _next_tool_index = 0
                            _emitted_ids = set()
                            continue
                        if _r2 is not None:
                            try:
                                _r2.close()
                            except Exception:
                                pass
                            log.warning("Stream follow-up got HTTP %s for '%s'; ending with text so far.",
                                        getattr(_r2, "status_code", "?"), model_name)
            elif event_type == "response.output_item.added":
                item = data.get("item") or {}
                if item.get("type") == "function_call":
                    output_index = data.get("output_index", 0)
                    key = item.get("id") or output_index
                    tool_call_state[key] = {
                        "index": _next_tool_index,
                        "upstream_index": output_index,
                        "id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:20]}",
                        "name": item.get("name") or "tool",
                    }
                    _next_tool_index += 1
            elif event_type == "response.output_item.done":
                item = data.get("item") or {}
                if item.get("type") == "function_call" and (item.get("call_id") or item.get("id")):
                    done_calls.append(dict(item))
            elif event_type == "response.function_call_arguments.delta":
                output_index = data.get("output_index", 0)
                key = data.get("item_id") or output_index
                state = tool_call_state.get(key) or tool_call_state.get(output_index)
                if not state:
                    state = {
                        "index": _next_tool_index,
                        "upstream_index": output_index,
                        "id": data.get("call_id") or data.get("item_id") or f"call_{uuid.uuid4().hex[:20]}",
                        "name": data.get("name") or "tool",
                    }
                    _next_tool_index += 1
                    tool_call_state[key] = state
                if followup is None:
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
                    _emitted_ids.add(state["id"])
                # 假 tools 场景（followup 非空）：调用块一律不播给下游
                #（下游没有真工具，执行不了会卡死），首轮攒着代理侧续问
            elif event_type == "error":
                _discard_stream_session = True
                break

        # 兜底：调用已知但零 chunk 播出（如无参数 delta）时合成播出，
        # 否则 finish=tool_calls 却无调用数据，下游无法执行而停住
        if followup is None:
            for _c in done_calls:
                if not (isinstance(_c, dict) and _c.get("type") == "function_call"):
                    continue
                _cid = _c.get("call_id") or _c.get("id")
                if not _cid or _cid in _emitted_ids:
                    continue
                _st = None
                for _s in tool_call_state.values():
                    if _s.get("id") == _cid:
                        _st = _s
                        break
                if _st is None:
                    _st = {"index": _next_tool_index, "id": _cid,
                           "name": _c.get("name") or "tool"}
                    _next_tool_index += 1
                    tool_call_state[_cid] = _st
                _args = _c.get("arguments")
                if not isinstance(_args, str):
                    _args = json.dumps(_args if isinstance(_args, (dict, list)) else {},
                                        ensure_ascii=False)
                yield make_chunk({"tool_calls": [{
                    "index": _st["index"], "id": _st["id"], "type": "function",
                    "function": {"name": _c.get("name") or _st.get("name") or "tool",
                                 "arguments": _args}}]})
                _emitted_ids.add(_cid)
        # 发 finish chunk（带过工具调用时 finish 必须为 tool_calls，否则下游忽略调用直接停住）
        if tool_call_state and finish_reason == "stop" and followup is None:
            finish_reason = "tool_calls"
        if followup is not None and _depth > 0 and _text_len == 0:
            # 续问的最终轮仍只调工具没文本：给一句兜底，避免下游只见首轮前言
            yield make_chunk({"content": "（本轮未能完成分析，请重试或补充说明。）"})
        if REQ_DIAG_ENABLED:
            log.info("stream-summary model=%s depth=%d text_len=%d head=%r calls=%d names=%s finish=%s completed=%s",
                     model_name, _depth, _text_len, _text_head, len(done_calls),
                     [c.get("name") for c in done_calls], finish_reason, saw_completed)
        yield make_chunk({}, finish_reason)
        yield b"data: [DONE]\n\n"

        # 更新 token usage（responses 流独立生成器，不经过 stream_response finally，
        # 这里是唯一补记点；无 usage 时沿用估算，保证不断记）
        track_token_usage(
            model_name,
            prompt_tokens=_pick_usage_num(usage, "input_tokens", "prompt_tokens", default=DEFAULT_PROMPT_TOKENS),
            completion_tokens=_pick_usage_num(usage, "output_tokens", "completion_tokens", default=DEFAULT_COMPLETION_TOKENS),
            count_request=False,  # 流开始时已计数，这里只补真实 token
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
    message_texts = []
    tool_calls_out = []
    for item in output:
        if item.get("type") == "reasoning":
            reasoning_text = item.get("encrypted_content", "") or ""
        elif item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text" and c.get("text"):
                    message_texts.append(c["text"])
                elif c.get("type") == "refusal" and c.get("refusal"):
                    message_texts.append(c["refusal"])
        elif item.get("type") == "function_call":
            # 上游（尤其被 shim 假 tools 诱发的视觉请求）可能带工具调用；
            # 必须透传给下游走 agent 循环，否则下游只见前言文本就停住。
            tool_calls_out.append({
                "id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:20]}",
                "type": "function",
                "function": {
                    "name": item.get("name") or "tool",
                    "arguments": item.get("arguments") if isinstance(item.get("arguments"), str) else json.dumps(item.get("arguments") or {}, ensure_ascii=False),
                },
            })
    message_text = "".join(message_texts)

    status = resp.get("status", "completed")
    incomplete = resp.get("incomplete_details") or {}
    if tool_calls_out:
        finish_reason = "tool_calls"
    elif status == "incomplete":
        reason = incomplete.get("reason", "")
        finish_reason = "length" if reason == "max_output_tokens" else "stop"
    else:
        finish_reason = "stop"

    message = {"role": "assistant", "content": message_text}
    if tool_calls_out:
        message["tool_calls"] = tool_calls_out
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

# session → proxy 绑定：不同会话使用不同代理出口
# 无上限增长 = 慢泄漏：只有失败分支 pop，正常会话永不清理。按 TTL 淘汰。
_session_proxy_map: Dict[str, tuple] = {}
_session_proxy_map_lock = threading.Lock()
SESSION_PROXY_TTL_SECONDS = max(60, int(os.environ.get("SESSION_PROXY_TTL_SECONDS", "3600")))
# public 匿名按会话绑定出口：OpenAI 兼容协议不带会话 ID，单一下游凭证只能看到
# 一个 session_key，因此用“新会话信号 + 长间隔”近似区分会话：
#  - 标题请求只在 OpenCode 新会话首条消息时发一次，是最准的新会话信号；
#  - 超过间隔无请求则视为新会话，换下一个出口（分摊各 IP 日配额）；
# 会话活跃期间稳定复用同一出口（亲和不断），429 另行即时换出口。
_anon_egress_lock = threading.Lock()
_anon_last_seen: Dict[str, float] = {}
ANON_SESSION_GAP_SECONDS = max(60, int(os.environ.get("ANON_SESSION_GAP_SECONDS", "600")))
# 按客户端标识缓存 x-opencode-session，同样加软上限防刷 key
_session_cache: Dict[str, str] = {}
_session_cache_lock = threading.Lock()
SESSION_CACHE_MAX_KEYS = max(100, int(os.environ.get("SESSION_CACHE_MAX_KEYS", "5000")))


def _session_proxy_get(session_key: str):
    with _session_proxy_map_lock:
        entry = _session_proxy_map.get(session_key)
        if entry is None:
            return None
        proxy, expires_at = entry
        if expires_at < time.monotonic():
            _session_proxy_map.pop(session_key, None)
            return None
        return proxy


def _session_proxy_set(session_key: str, proxy_url: str) -> None:
    with _session_proxy_map_lock:
        if len(_session_proxy_map) >= SESSION_CACHE_MAX_KEYS and session_key not in _session_proxy_map:
            # O(1) FIFO 淘汰最早插入的 key：dict 保持插入序，避免 min() 全表 O(N) 扫描
            try:
                _session_proxy_map.pop(next(iter(_session_proxy_map)), None)
            except StopIteration:
                pass
        _session_proxy_map[session_key] = (proxy_url, time.monotonic() + SESSION_PROXY_TTL_SECONDS)


def _session_proxy_pop(session_key: str) -> None:
    with _session_proxy_map_lock:
        _session_proxy_map.pop(session_key, None)


# --- 空流换出口 -------------------------------------------------------------
# 上游偶发回空流（如 space-bunny 06:33 连空 8 次）：空流重试原来固定走同一出口
#（会话亲和），坏窗口里 4 次必全灭。这里按会话累计连续空流，达到阈值就把绑定
# 推进到池内下一个出口，重试打散；任一次成功即清零。只影响强制流式聚合路径
#（下游非流），下游流式请求不在重试循环内，不涉及。
EMPTY_STREAM_ROTATE_THRESHOLD = max(1, int(os.environ.get("EMPTY_STREAM_ROTATE_THRESHOLD", "2")))
EMPTY_STREAM_ROTATE_ENABLED = os.environ.get("EMPTY_STREAM_ROTATE", "true").lower() in ("true", "1", "yes")
_empty_stream_strikes: Dict[str, int] = {}
_empty_stream_strikes_lock = threading.Lock()


def _note_empty_stream(session_key: str, model: str) -> None:
    """记录一次空流；连续达到阈值时将会话绑定推进到下一个出口。"""
    if not EMPTY_STREAM_ROTATE_ENABLED or not session_key:
        return
    with _empty_stream_strikes_lock:
        if len(_empty_stream_strikes) >= SESSION_CACHE_MAX_KEYS and session_key not in _empty_stream_strikes:
            try:
                _empty_stream_strikes.pop(next(iter(_empty_stream_strikes)), None)
            except StopIteration:
                pass
        strikes = _empty_stream_strikes.get(session_key, 0) + 1
        _empty_stream_strikes[session_key] = strikes
    if strikes < EMPTY_STREAM_ROTATE_THRESHOLD:
        return
    try:
        cur = _session_proxy_get(session_key) or proxy_pool.active_proxy()
        nxt = proxy_pool.next_proxy_after(cur, model=model)
        if nxt and nxt != cur:
            _session_proxy_set(session_key, nxt)
            log.warning("Consecutive empty streams x%s for '%s'; egress %s -> %s.",
                        strikes, model, cur, nxt)
        else:
            # 池内无其他可用出口：解绑让下次重选（可能回到同一出口）
            _session_proxy_pop(session_key)
            log.warning("Consecutive empty streams x%s for '%s'; no alternate egress, unbound session.",
                        strikes, model)
    except Exception as exc:
        log.warning("Empty-stream egress advance failed: %s", exc)
    finally:
        with _empty_stream_strikes_lock:
            _empty_stream_strikes.pop(session_key, None)


def _reset_empty_stream_strikes(session_key: str) -> None:
    """上游成功返回一次即清零（打断连续计数）。"""
    if not session_key:
        return
    with _empty_stream_strikes_lock:
        _empty_stream_strikes.pop(session_key, None)

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
    # 恢复持久化计数器；老版本从未落盘，用 model_usage 求和兜底，避免重建后归零
    try:
        saved_counters = load_global_counters()
        for _k in PERSISTED_COUNTER_KEYS:
            if _k in saved_counters:
                metrics[_k] = max(int(metrics.get(_k, 0) or 0), int(saved_counters[_k] or 0))
        _usage_sum = sum(int((v or {}).get("requests", 0) or 0) for v in model_usage_stats.values())
        if _usage_sum > 0:
            if int(metrics.get("total_requests", 0) or 0) < _usage_sum:
                metrics["total_requests"] = _usage_sum
            if int(metrics.get("successful_requests", 0) or 0) < _usage_sum:
                # 成功数与用量统计口径接近，至少不小于用量和，避免同样归零
                metrics["successful_requests"] = max(
                    int(metrics.get("successful_requests", 0) or 0),
                    min(_usage_sum, int(metrics.get("total_requests", 0) or 0)),
                )
    except Exception as exc:
        log.warning("restore global counters failed: %s", exc)
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
                    if REQ_DIAG_ENABLED:
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

def _fetch_upstream_models():
    """通过当前出口代理拉取上游模型列表。

    2026-10-14：原实现用 urllib 直连 opencode.ai，每 5 分钟一次（288 次/天）全部
    走 VPS 裸 IP，且没有 TLS 指纹伪装，基本会被上游静默拒绝（只落 debug 日志）。
    改为走代理池出口 + chrome124 伪装；无可用出口时返回 None，保留上次列表。
    """
    disc_headers = get_realistic_headers()
    proxy_url = proxy_pool.active_proxy() or proxy_pool.select_active_proxy()
    if not proxy_url:
        log.debug("Auto-Discovery skipped: no egress proxy available; keeping previous model list.")
        return None
    proxies = {"http": proxy_url, "https": proxy_url}
    # 依次尝试新端点与旧的公开列表端点（2026-10-15：匿名推理走新端点，
    # 但模型列表在旧端点仍公开可读，作为兜底）。
    for base in TARGET_MODELS_BASES:
        try:
            resp = cffi_requests.get(
                f"{base}/models",
                headers=disc_headers,
                impersonate="chrome124",
                proxies=proxies,
                timeout=10,
            )
        except Exception as exc:
            log.debug(f"Auto-Discovery {base}/models failed via {proxy_url}: {exc}")
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                continue
        log.debug(f"Auto-Discovery upstream HTTP {resp.status_code} via {proxy_url} ({base}).")
    return None


def discover_models_task():
    global discovered_models
    while not _discovery_stop.is_set():
        try:
            data = _fetch_upstream_models()
            if data:
                models_data = data.get("data", [])
                if models_data:
                    new_models = []
                    for m in models_data:
                        m_id = m.get("id", "")
                        if "free" in m_id.lower() or "zen" in m_id.lower():
                            new_models.append({"id": m_id, "name": m_id.replace("-", " ").title()})

                    if new_models:
                        # 过滤掉已知不可用的模型（见 UNAVAILABLE_MODELS）
                        new_models = [m for m in new_models if m["id"] not in UNAVAILABLE_MODELS]
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

_id_counter = 0


def _random_opencode_id(prefix: str, descending: bool = True) -> str:
    """生成 OpenCode CLI 格式的 ID，如 msg_xxx / ses_xxx
    格式：前缀 + 12位hex时间戳 + 14位随机base62 = 30字符
    与 opencode 源码 packages/schema/src/identifier.ts 一致。

    2026-10-15：counter 必须落在 1..63（官方实现同毫秒自增、跨毫秒归零）。
    上游会解码 12 位时间前缀做校验，counter=0 会与官方形状对不上。"""
    import time
    import secrets
    global _id_counter
    timestamp_ms = int(time.time() * 1000)
    _id_counter = (_id_counter % 63) + 1
    current = (timestamp_ms * 0x1000) + _id_counter
    value = ~current if descending else current
    time_hex = format(value & 0xffffffffffff, '012x')
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    rand = ''.join(secrets.choice(chars) for _ in range(14))
    return f"{prefix}_{time_hex}{rand}"



# 2026-10-15：上游按 OpenCode 客户端指纹放行。UA 必须是 opencode/<channel>/<version>/cli
# 形状——v1 风格的 "opencode/1.18.31 ai-sdk/..." 会被直接 403。版本号本身不校验。
OPENCODE_CLIENT_UA = os.environ.get("OPENCODE_CLIENT_UA", "opencode/latest/2.0.29/cli")


def _client_project_id(project: str = "global") -> str:
    """上游拒绝 canonical 的 prj_* 形状，只接受客户端自己的裸 40 位小写 hex。
    值本身不校验、只校验形状；哈希保证同一会话稳定绑定同一 project。"""
    import hashlib
    return hashlib.sha256(b"opencode-project\x00" + project.encode()).hexdigest()[:40]


# ── 下游客户端识别（2026-10-15）────────────────────────────────────────────
# 目的：让请求历史 / trace 能区分「谁在调」——opencode CLI、codex、curl、面板等。
#
# 关键约束：绝不能用 get_realistic_headers() 里那套头来判断客户端。那是网关
# 发给【上游】的伪装指纹（User-Agent=opencode/.../cli、x-opencode-client=cli），
# 无论真实客户端是谁都会是这些值，用它判断会把所有请求都认成 opencode。
# 正确来源是 raw_request.headers，即下游真正发来的头。
#
# 另注：三个端点都会把下游的 x-opencode-* 头原样透传给上游（L4313 等），
# 下游若带 x-opencode-request 会直接触发上游 403 闸门，所以识别逻辑只「读」
# 这些头，绝不「写」，保持现有透传行为不变。
CLIENT_PROBE_ENABLED = os.environ.get(
    "CLIENT_PROBE", "0").lower() in ("true", "1", "yes")

# 指纹头白名单：只取这些用于识别，避免把 Authorization / Cookie 之类
# 敏感或高基数字段带进来。
_CLIENT_FINGERPRINT_HEADERS = (
    "user-agent",
    "x-opencode-client",
    "x-opencode-version",
    "x-opencode-session",
    "x-stainless-lang",
    "x-stainless-package-version",
    "x-stainless-os",
    "x-stainless-arch",
    "x-stainless-runtime",
    "x-stainless-runtime-version",
    "x-stainless-retry-count",
    "x-client-type",
    "x-source",
    "x-app",
    "x-request-source",
    "originator",
)


def _redact_client_header_value(key, value):
    """会话类 id 只留形状，不落原文（避免把可关联标识写进日志库）。"""
    low = key.lower()
    if "session" in low:
        v = value.strip()
        if len(v) > 8:
            return v[:4] + "..." + v[-4:]
        return v
    return value


def _fingerprint_downstream_client(raw_request, body=None):
    """收集下游客户端指纹快照（仅 CLIENT_PROBE=1 时调用）。

    返回一行紧凑 JSON，值已截断/脱敏。用于识别规则上线前的规则校准，
    以及将来遇到未知客户端时的兜底取证。
    """
    try:
        snap = {}
        for key in _CLIENT_FINGERPRINT_HEADERS:
            val = raw_request.headers.get(key)
            if not val:
                continue
            snap[key] = _redact_client_header_value(key, val)[:120]
        # body 侧信号：部分 SDK 在 metadata 里放 user_agent
        if isinstance(body, dict):
            _meta = body.get("metadata")
            if isinstance(_meta, dict):
                for _mk in ("user_agent", "client", "source"):
                    if _meta.get(_mk):
                        snap["body.metadata." + _mk] = str(_meta[_mk])[:80]
        if not snap:
            return ""
        return json.dumps(snap, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return ""


def detect_client(raw_request, body=None):
    """识别真实下游客户端，返回短标签。

    只读 raw_request.headers（下游真发来的头），不碰伪装给上游的那套头。
    判定优先级：显式声明 > SDK 指纹 > UA 关键字 > 无 UA 兜底。
    """
    try:
        h = raw_request.headers
        ua = (h.get("user-agent") or "").strip()
        ua_low = ua.lower()

        # 1) 客户端主动声明（最可信）
        declared = (h.get("x-opencode-client") or "").strip().lower()
        if declared:
            return declared[:32]
        declared2 = (h.get("x-client-type") or h.get("x-app")
                     or h.get("x-request-source") or h.get("originator") or "").strip().lower()
        if declared2:
            return declared2[:32]

        # 2) Stainless 系 SDK 指纹（codex / openai-python 等都用）
        if h.get("x-stainless-lang"):
            if "codex" in ua_low:
                return "codex"
            return "openai-sdk"

        # 3) UA 关键字
        for _kw, _label in (
            # 实测 2026-10-15：本机主力客户端是 WorkBuddy，UA 形如
            # "WorkBuddy/5.7.6 WorkBuddy/5.7.6 CLI/2.156.0"
            ("workbuddy", "workbuddy"),
            ("codebuddy", "codebuddy"),
            ("codex", "codex"),
            ("openai-", "codex"),
            ("opencode", "opencode"),
            ("claude-cli", "claude-code"),
            ("claude-code", "claude-code"),
            ("anthropic", "claude-code"),
            ("cline", "cline"),
            ("roo-code", "roo-code"),
            ("kilocode", "kilocode"),
            ("continue", "continue"),
            ("aider", "aider"),
            ("curl", "curl"),
            ("python-requests", "python-requests"),
            ("httpx", "python-httpx"),
            ("openai-node", "openai-node"),
            ("undici", "node-undici"),
            ("node-fetch", "node"),
        ):
            if _kw in ua_low:
                return _label
        if ua_low.startswith("mozilla") or ua_low.startswith("chrome"):
            return "browser"

        # 4) 无 UA 线索
        if not ua_low:
            return "no-ua"
        return "other"
    except Exception:
        return "unknown"


def get_realistic_headers(client_key: str = "") -> Dict[str, str]:
    if client_key:
        with _session_cache_lock:
            if client_key not in _session_cache:
                if len(_session_cache) >= SESSION_CACHE_MAX_KEYS:
                    _session_cache.pop(next(iter(_session_cache)), None)
                _session_cache[client_key] = _random_opencode_id("ses", descending=True)
            session = _session_cache[client_key]
    else:
        env_ses = os.environ.get("OPENCODE_SESSION", "")
        if env_ses and re.match(r"^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$", env_ses):
            session = env_ses
        else:
            session = _random_opencode_id("ses", descending=True)
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer public",
        "Accept": "application/json, text/event-stream, */*",
        "User-Agent": OPENCODE_CLIENT_UA,
        "x-opencode-client": "cli",
        # 上游 2026-10-14 起：canonical 的 prj_* 会 403，只认裸 40-hex 形状
        "x-opencode-project": _client_project_id(),
        # 注意：x-opencode-request 自 2026-10-14 起任何非空值都会 403，必须不发。
        # 同一 client_key 复用同一 session：原来每次随机，亲和失效 + map 堆垃圾。
        # 无 key 匿名请求保持每次随机（无法区分客户端，随机反而能分散代理）。
        "x-opencode-session": session,
        # 上游要求两个 session 头同值；缺 x-opencode-session-id 会被 403
        "x-opencode-session-id": session,
        "x-session-affinity": session,
        "X-Session-Id": session,
    }


UPSTREAM_API_KEY_OVERRIDE = os.environ.get("UPSTREAM_API_KEY", "").strip()


def apply_downstream_auth(headers, raw_request):
    try:
        # 服务端强制替换：配置了 UPSTREAM_API_KEY 时（包括 public 匿名模式），一律用配置的值上游
        if UPSTREAM_API_KEY_OVERRIDE:
            if UPSTREAM_API_KEY_OVERRIDE.lower() == "public":
                headers["Authorization"] = "Bearer public"
                return headers
            _k = UPSTREAM_API_KEY_OVERRIDE
            headers["Authorization"] = _k if _k.lower().startswith("bearer ") else "Bearer " + _k
            return headers
        import hashlib as _hl
        auth = raw_request.headers.get("authorization", "") or ""
        api_key = raw_request.headers.get("x-api-key", "") or ""
        _cred = (auth or api_key or "").strip()
        if _cred:
            _fp = _hl.sha256(_cred.encode()).hexdigest()[:12]
            log.warning("downstream credential seen src=%s fp=%s len=%d",
                        "authorization" if auth else "x-api-key", _fp, len(_cred))
        if auth and auth.strip().lower() != "bearer public":
            headers["Authorization"] = auth.strip()
        elif api_key and api_key.strip().lower() != "public":
            v = api_key.strip()
            headers["Authorization"] = v if v.lower().startswith("bearer ") else "Bearer " + v
    except Exception:
        pass
    return headers


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


# 单次请求内因 429 轮换出口的次数（凭据级限流判定用）。
#
# 必须用 ContextVar 而非模块级 global：asyncio 单线程但协程会交错，
# 模块级变量会让 A 请求的轮换次数被 B 请求读到，导致 B 误判「已轮换过」
# 而放弃本该有效的轮换。每个请求在 attempt 循环开始时 reset()。
_rate_limit_rotations: contextvars.ContextVar = contextvars.ContextVar(
    "rate_limit_rotations", default=0)


def _reset_rate_limit_rotations() -> None:
    _rate_limit_rotations.set(0)


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
    session_key: str = "",
    current_proxy: str = "",
    rotation_used: bool = False,
) -> bool:
    """Retry a transient 429 once on the same IP before considering rotation.

    rotation_used: 本次请求此前是否已因 429 轮换过出口。若已轮换过仍然 429，
    说明限流是凭据级而非出口级，此时快速失败（返回 False）让调用方透传 429，
    不再继续轮换把健康出口拖进冷却。
    """
    # ── 凭据级限流：已轮换过出口仍 429 ──
    if rotation_used:
        if CREDENTIAL_LIMIT_SKIP_QUOTA:
            # 凭据级限流不是这个出口的配额问题，记冷却会误伤健康出口
            log.warning(
                "429 for '%s' on %s persists AFTER egress rotation -> treating as "
                "credential-level limit (not per-egress); passing through without "
                "further rotation.",
                model_name, egress_key,
            )
        else:
            try:
                proxy_pool.record_upstream_quota(
                    current_proxy or None, model_name, None, "credential_limit")
            except Exception as exc:
                log.warning("Record quota failed: %s", exc)
        return False

    category, retry_after, _ = classify_upstream_429(response)
    if category == "quota":
        # 日配额类 429：记下该出口该模型的冷却，供选路和面板使用
        try:
            proxy_pool.record_upstream_quota(current_proxy or None, model_name, retry_after, "quota")
        except Exception as exc:
            log.warning("Record quota failed: %s", exc)
        return False
    if retry_after is not None and retry_after > LONG_BAN_THRESHOLD_SECONDS:
        if (UPSTREAM_API_KEY_OVERRIDE.lower() == "public" and session_key
                and attempt < MAX_RETRIES_ON_429):
            # public 匿名模式配额按出口 IP 算（日配额、retry-after 到午夜清零），
            # 长封禁只代表当前出口没额度，换池内下一个可用出口再试。key 模式
            # 仍走下面的原逻辑（换 IP 无用，直接透传）。
            nxt = None
            try:
                nxt = proxy_pool.next_proxy_after(current_proxy or None, model=model_name)
            except Exception as exc:
                log.warning("Public-mode egress reselect failed: %s", exc)
                nxt = None
            if nxt and nxt != (current_proxy or None):
                try:
                    proxy_pool.record_upstream_quota(current_proxy or None, model_name, retry_after, "long_ban")
                except Exception as exc:
                    log.warning("Record quota failed: %s", exc)
                _session_proxy_set(session_key, nxt)
                with _anon_egress_lock:
                    _anon_last_seen[session_key] = time.monotonic()
                if stream_pool_key is not None:
                    _release_stream_session(stream_pool_key, session, discard=True)
                else:
                    _reset_request_session(endpoint, session, pooled=pooled)
                log.warning(
                    "Long upstream ban (retry_after=%ss) on %s is per-egress quota in public mode; "
                    "session re-bound %s -> %s. Retrying (%s/%s).",
                    retry_after, egress_key, current_proxy, nxt, attempt, MAX_RETRIES_ON_429,
                )
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
                return True
            log.warning(
                "Long upstream ban for '%s' (retry_after=%ss > %ss); no other egress available, passing through.",
                model_name, retry_after, LONG_BAN_THRESHOLD_SECONDS,
            )
            return False
        # key/账号级长封禁（如 retry-after 数小时）：换出口无用，不重试不轮换，
        # 直接返回 False 让调用方把 429 + Retry-After 透给下游退避。
        # 配额照记（供面板可见；key 模式下选路不依赖它）。
        try:
            proxy_pool.record_upstream_quota(current_proxy or None, model_name, retry_after, "long_ban")
        except Exception as exc:
            log.warning("Record quota failed: %s", exc)
        log.warning(
            "Long upstream ban for '%s' (retry_after=%ss > %ss); "
            "skipping retry and egress rotation.",
            model_name, retry_after, LONG_BAN_THRESHOLD_SECONDS,
        )
        return False

    streak, rotate_now = _egress_rate_policy.record_rate_limit(egress_key)
    if not rotate_now:
        requested_delay = retry_after if retry_after is not None else 0.0
        delay = min(requested_delay, MAX_RATE_LIMIT_WAIT)
        if retry_after is None:
            # 上游未给 Retry-After：用退避下限，避免 0s 立即重试把上游打爆
            delay = max(delay, RATE_LIMIT_MIN_BACKOFF_SECONDS)
        try:
            proxy_pool.record_upstream_quota(current_proxy or None, model_name, retry_after, "rate_limit")
        except Exception as exc:
            log.warning("Record quota failed: %s", exc)
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
    # 轮换预算用尽：再换也只是徒劳（凭据级限流），直接失败让下游退避
    _rl_used = _rate_limit_rotations.get()
    if _rl_used >= CREDENTIAL_LIMIT_ROTATION_BUDGET:
        log.warning(
            "429 for '%s' on %s; egress-rotation budget (%d/%d) already spent -> "
            "likely credential-level limit, passing through.",
            model_name, egress_key, _rl_used, CREDENTIAL_LIMIT_ROTATION_BUDGET,
        )
        return False

    # 给被 429 的旧出口记冷却，避免换出口后它立刻又被选中（否则会秒级横扫出口池）
    try:
        proxy_pool.record_upstream_quota(current_proxy or None, model_name, retry_after, "rate_limit")
    except Exception as exc:
        log.warning("Record quota failed: %s", exc)

    rotated, new_ip = await rotate_egress_safely(
        f"confirmed HTTP 429 x{streak} (attempt {attempt}/{MAX_RETRIES_ON_429})"
    )
    if not rotated:
        log.warning("Confirmed HTTP 429 for '%s', but egress rotation failed.", model_name)
        return False

    _reset_request_session(endpoint, session, pooled=pooled, stream_pool_key=stream_pool_key)
    _rl_used = _rate_limit_rotations.get() + 1
    _rate_limit_rotations.set(_rl_used)
    if RATE_LIMIT_ROTATE_BACKOFF_SECONDS:
        await asyncio.sleep(RATE_LIMIT_ROTATE_BACKOFF_SECONDS)
    log.warning(
        "Confirmed HTTP 429 for '%s'; egress rotated -> %s (%d/%d). Retrying in %.1fs.",
        model_name, new_ip, _rl_used,
        CREDENTIAL_LIMIT_ROTATION_BUDGET, RATE_LIMIT_ROTATE_BACKOFF_SECONDS,
    )
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

def _rewrite_tool_call_chunk(line: bytes, state: dict) -> list:
    """客户端未带 tools 时，把 chat SSE 里的 tool_calls 改写成纯文本。

    背景：为过上游 403 闸门，网关会注入"官方向量"工具定义，上游模型因此可能中途
    发起工具调用并以 finish_reason=tool_calls 收尾、正文为空。普通聊天客户端拿不到
    可执行的工具就会一直停住（表现为"AI 干一半卡死"）。这里把工具调用吞掉、转成
    一段说明文本，并把收尾改成 stop，保证下游永远收得到正文、不会悬停。

    返回 list[bytes]（每行不含尾换行）：可能为空（吞掉该行）、1 行或 2 行。
    """
    raw = line.decode("utf-8", errors="ignore").strip()
    if not raw.startswith("data:"):
        return [line]
    body = raw[5:].strip()
    if not body or body == "[DONE]":
        return [line]
    try:
        d = json.loads(body)
    except (TypeError, ValueError):
        return [line]
    if not isinstance(d, dict):
        return [line]
    choices = d.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return [line]
    ch = choices[0]
    delta = ch.get("delta")

    # 1) 工具调用分片：累积后吞掉，不向下游暴露
    if isinstance(delta, dict) and delta.get("tool_calls"):
        for tc in delta["tool_calls"]:
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function")
            if not isinstance(fn, dict):
                continue
            if fn.get("name"):
                state["names"].append(str(fn["name"]))
                state["args"].append("")
            arg = fn.get("arguments")
            if isinstance(arg, str) and arg:
                if state["args"]:
                    state["args"][-1] += arg
                else:
                    state["args"].append(arg)
        return []

    # 2) 收尾为 tool_calls：补一段说明文本，并把 finish_reason 改成 stop
    if ch.get("finish_reason") == "tool_calls":
        parts = []
        for i, nm in enumerate(state["names"]):
            arg = state["args"][i] if i < len(state["args"]) else ""
            arg = arg.strip().replace("\n", " ")
            if len(arg) > 300:
                arg = arg[:300] + "…"
            parts.append("`%s(%s)`" % (nm, arg))
        if isinstance(delta, dict):
            delta.pop("tool_calls", None)
        else:
            ch["delta"] = {}
        ch["finish_reason"] = "stop"
        out = []
        if parts:
            head = json.loads(json.dumps(d))
            hch = head["choices"][0]
            hch["delta"] = {"role": "assistant",
                            "content": "\n\n（模型尝试调用工具 %s，但当前接口未启用工具执行，已省略。）"
                                       % "、".join(parts)}
            hch["finish_reason"] = None
            out.append(("data: " + json.dumps(head, ensure_ascii=False)).encode("utf-8"))
        out.append(("data: " + json.dumps(d, ensure_ascii=False)).encode("utf-8"))
        return out

    return [line]


async def stream_response(response, model_name: str, session=None, protocol: str = "chat", stream_pool_key=None, suppress_tool_calls: bool = False) -> AsyncGenerator[bytes, None]:
    """Forward SSE and normalize terminal events for the selected API protocol."""
    loop = asyncio.get_event_loop()
    global active_flows_count
    is_responses_api = protocol == "responses"
    is_anthropic_api = protocol == "anthropic"
    # 客户端未带 tools 时为 True：上游（被我们为过闸门注入的官方工具定义诱导）可能
    # 中途发 tool_calls 并以 finish_reason=tool_calls 结束，正文为空 → 下游普通聊天
    # 客户端接不住工具调用会一直卡住。此处把工具调用改写成文本并收尾为 stop。
    _tool_call_state = {"names": [], "args": []} if (suppress_tool_calls and not is_responses_api and not is_anthropic_api) else None
    # 正常播完才归还 keepalive；中途断开/异常则丢弃，避免脏连接污染池
    _discard_stream_session = False

    with flow_lock:
        active_flows_count += 1
        prom_active_flows.set(active_flows_count)
    # 2026-10-15 修复：客户端可能在建租约这一步之前就断开，此处的 await 会立即
    # 重抛取消（GeneratorExit/CancelledError），而后面的 try/finally 尚未进入 ——
    # 计数就永远不回落。实测每次断流泄漏 +1（此前 403 快速失败时不会挂在 await
    # 上，故被掩盖）。这里兜住取消：先递减再原样抛出，取消语义不变但计数闭环。
    try:
        lease_id = await asyncio.to_thread(acquire_flow_lease)
    except BaseException:
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)
        raise

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
    # chat 协议流尾部 usage chunk（上游带 stream_options.include_usage 时必有）：
    # inspect_line 透传时顺手捕获，finally 统一补记真实 token。
    chat_usage: dict = {}
    # thinking 模型回传缓存：本条响应攒 reasoning 文本 + tool_call id，
    # finally 里 put 进会话缓存（下一轮客户端缺失时由 inject 补齐）
    _reason_buf: list = []
    _tc_ids_seen: set = set()

    def inspect_line(line: bytes) -> tuple[str, Optional[dict], str]:
        nonlocal has_meaningful_content, response_completed_seen, latest_response, pending_sse_event
        nonlocal anthropic_message_id, anthropic_stop_reason, anthropic_stop_sequence, anthropic_usage
        nonlocal chat_usage
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
            # chat 流尾部 usage chunk（含 stream_options.include_usage 时必有）：
            # 透传同时捕获，finally 统一补记真实 token。
            if '"usage"' in raw:
                try:
                    _data_text = raw[5:].strip() if raw.startswith("data:") else raw
                    _chunk = json.loads(_data_text)
                    _u = _chunk.get("usage") if isinstance(_chunk, dict) else None
                    if isinstance(_u, dict) and _u:
                        chat_usage.update(_u)
                except (TypeError, ValueError):
                    pass
            # thinking 模型思维捕获：攒 reasoning 文本 + 记录 tool_call id，
            # finally 统一写入会话级缓存供下一轮回传注入。
            if '"reasoning_content"' in raw or '"tool_calls"' in raw or '"reasoning"' in raw:
                try:
                    _data_text = raw[5:].strip() if raw.startswith("data:") else raw
                    _chunk = json.loads(_data_text)
                    if isinstance(_chunk, dict):
                        for _cch in _chunk.get("choices") or []:
                            _cd = _cch.get("delta") or {}
                            _rv = _cd.get("reasoning_content") or _cd.get("reasoning")
                            if isinstance(_rv, str) and _rv:
                                _reason_buf.append(_rv)
                            for _tc in _cd.get("tool_calls") or []:
                                if isinstance(_tc, dict) and _tc.get("id"):
                                    _tc_ids_seen.add(str(_tc["id"]))
                except (TypeError, ValueError):
                    pass
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
                if REQ_DIAG_ENABLED:
                    log.error(
                        "[STREAM DEBUG] Upstream socket/connection error for '%s': %s: %s",
                        model_name, type(exc).__name__, exc,
                    )
                else:
                    # 真实错误保留一行简报，详细堆栈仅在 REQ_DIAG=1 时输出
                    log.warning("Upstream stream socket error for '%s': %s", model_name, type(exc).__name__)
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
            # 单次解析复用：inspect_line 内部已调 _parse_stream_event，直接用 event_type
            # 判 __done__，避免 is_done_sentinel() 对同一行二次 json.loads
            _ev, _, _delta = inspect_line(line)
            if _ev == "__done__":
                continue
            chunk_count += 1
            if is_anthropic_api:
                if _delta:
                    yield anthropic_delta_event(_delta)
            elif _tool_call_state is None:
                yield line + b"\n"
            else:
                for _rl in _rewrite_tool_call_chunk(line, _tool_call_state):
                    yield _rl + b"\n"

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
            _ev, _, delta = inspect_line(item)
            # 同上：复用本次解析结果判 __done__，省一次 json.loads
            if _ev == "__done__":
                continue
            chunk_count += 1
            if is_anthropic_api:
                if delta:
                    yield anthropic_delta_event(delta)
            elif _tool_call_state is None:
                yield item + b"\n"
            else:
                for _rl in _rewrite_tool_call_chunk(item, _tool_call_state):
                    yield _rl + b"\n"

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
        if REQ_DIAG_ENABLED:
            log.warning(
                "[STREAM DEBUG] Client explicitly closed/aborted SSE for '%s' after %s lines.",
                model_name, chunk_count,
            )
    except Exception as exc:
        _discard_stream_session = True
        log.error("Stream exception for '%s': %s: %s", model_name, type(exc).__name__, exc, exc_info=True)
        if is_anthropic_api:
            yield b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"stream_error\",\"message\":\"Upstream stream interrupted\"}}\n\n"
        elif not is_responses_api:
            yield b"\ndata: [DONE]\n\n"
    finally:
        # 取消安全（2026-10-14 修复）：客户端断开时任务被取消，finally 中的 await 会
        # 立即重抛 CancelledError，导致其后的计数递减 / 租约释放 / 连接归还全部被
        # 跳过——实测每次断流 active_flows 泄漏 +1 且永不回落，死租约还会拖延
        # rotator 的轮换判定（最多 90s TTL）。修法：同步清理先于任何 await 执行，
        # 所有 await 包 BaseException 吞掉取消重抛，保证清理完整。
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
        # 流结束补记真实 token：入口处只计了请求数（0/0），这里补真实 usage
        #（count_request=False 不重数）；上游无 usage 事件时沿用估算，保证不断记。
        try:
            _real_usage = None
            if is_responses_api:
                _lr = latest_response if isinstance(latest_response, dict) else None
                _lu = (_lr or {}).get("usage")
                _real_usage = _lu if isinstance(_lu, dict) and _lu else None
            elif is_anthropic_api:
                _real_usage = anthropic_usage or None
            else:
                _real_usage = chat_usage or None
            track_token_usage(
                model_name,
                prompt_tokens=_pick_usage_num(_real_usage, "prompt_tokens", "input_tokens", default=DEFAULT_PROMPT_TOKENS),
                completion_tokens=_pick_usage_num(_real_usage, "completion_tokens", "output_tokens", default=DEFAULT_COMPLETION_TOKENS),
                count_request=False,
            )
        except Exception:
            pass
        # thinking 模型回传缓存：把本响应的思维文本挂到 tool_call id 上
        if not is_responses_api and not is_anthropic_api and _tc_ids_seen and _reason_buf:
            try:
                _reason_text = "".join(_reason_buf)
                if _reason_text:
                    for _tid in _tc_ids_seen:
                        _reasoning_cache_put(_tid, _reason_text)
            except Exception:
                pass
        lease_heartbeat.cancel()
        try:
            await asyncio.gather(lease_heartbeat, return_exceptions=True)
        except BaseException:
            pass
        try:
            await asyncio.to_thread(release_flow_lease, lease_id)
        except BaseException:
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
def api_request_history(limit: int = 100, client: str = ""):
    """Return recent request history records.

    2026-10-15：新增 client 列（opencode / codex / curl / browser / no-ua ...），
    用来区分「谁在调」；client 非空时只返回该客户端的记录。
    """
    try:
        sql = ("SELECT id, timestamp, model, proxy_ip, egress_ip, latency_ms, "
               "status, is_stream, attempt, error_msg, client "
               "FROM request_history")
        params = []
        if client:
            sql += " WHERE client = ?"
            params.append(client)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(min(limit, 500))
        rows = _db_fetchall(sql, tuple(params))
        result = []
        for row in rows:
            if hasattr(row, 'keys'):
                result.append(dict(row))
            else:
                result.append({
                    "id": row[0], "timestamp": row[1], "model": row[2],
                    "proxy_ip": row[3], "egress_ip": row[4], "latency_ms": row[5],
                    "status": row[6], "is_stream": row[7], "attempt": row[8],
                    "error_msg": row[9], "client": row[10] if len(row) > 10 else "",
                })
        return {"ok": True, "history": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/panel/clients")
def api_client_breakdown(since: str = "", limit: int = 20000):
    """按客户端聚合请求数/成功率/平均耗时，用于快速看谁在调、调得好不好。"""
    try:
        sql = ("SELECT COALESCE(NULLIF(client, ''), 'unknown'), "
               "COUNT(*), "
               "SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END), "
               "AVG(latency_ms), "
               "SUM(CASE WHEN status!='ok' THEN 1 ELSE 0 END) "
               "FROM request_history")
        params = []
        if since:
            sql += " WHERE timestamp >= ?"
            params.append(since)
        sql += " GROUP BY 1 ORDER BY 2 DESC LIMIT ?"
        params.append(min(limit, 500))
        rows = _db_fetchall(sql, tuple(params))
        out = []
        for row in rows:
            # 显式按位置取，不依赖 sqlite 对列别名的处理
            out.append({
                "client": row[0],
                "requests": row[1],
                "ok": row[2],
                "avg_ms": round(row[3], 1) if row[3] is not None else None,
                "failed": row[4],
            })
        return {"ok": True, "clients": out}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/panel/trace")
def api_request_trace(limit: int = 200, since: str = "", model: str = "",
                      only_slow: int = 0, only_problem: int = 0, client: str = ""):
    """单请求级 trace（2026-10-15 埋点）：TTFT / 输出量 / finish_reason / 错误类型。

    query:
      limit          返回条数（<=500）
      since          起始时间 'YYYY-MM-DD HH:MM:SS'
      model          精确匹配模型名
      only_slow      1 = 只返回 total_ms > 30000 或 attempt > 1 的
      only_problem   1 = 只返回 status != ok / error_type 非空 / downstream_tool_calls=1
      client         精确匹配客户端标签（opencode / codex / curl / ...）
    """
    try:
        where, params = [], []
        if since:
            where.append("ts >= ?")
            params.append(since)
        if model:
            where.append("model = ?")
            params.append(model)
        if only_slow:
            where.append("(total_ms > 30000 OR attempt > 1)")
        if only_problem:
            where.append("(status != 'ok' OR (error_type IS NOT NULL AND error_type != '') "
                         "OR downstream_tool_calls = 1)")
        if client:
            where.append("client = ?")
            params.append(client)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        params.append(min(limit, 500))
        rows = _db_fetchall(
            "SELECT id, ts, model, endpoint, client, is_stream, client_tools, proxy_ip, egress_ip, "
            "ttft_ms, total_ms, chunks, out_bytes, status, attempt, finish_reason, "
            "downstream_tool_calls, upstream_status, error_type, prompt_tokens, completion_tokens "
            "FROM request_trace" + clause + " ORDER BY id DESC LIMIT ?",
            tuple(params),
        )
        out = []
        for row in rows:
            if hasattr(row, "keys"):
                out.append(dict(row))
            else:
                out.append({
                    "id": row[0], "ts": row[1], "model": row[2], "endpoint": row[3],
                    "client": row[4],
                    "is_stream": row[5], "client_tools": row[6], "proxy_ip": row[7],
                    "egress_ip": row[8], "ttft_ms": row[9], "total_ms": row[10],
                    "chunks": row[11], "out_bytes": row[12], "status": row[13],
                    "attempt": row[14], "finish_reason": row[15],
                    "downstream_tool_calls": row[16], "upstream_status": row[17],
                    "error_type": row[18], "prompt_tokens": row[19],
                    "completion_tokens": row[20],
                })
        return {"ok": True, "count": len(out), "trace": out}
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
            snapshot["model_pricing"] = MODEL_PRICING
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
        "model_pricing": MODEL_PRICING,
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
        "total_rotations": int(state.get("rotations") or 0),
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
    _client_tag = detect_client(raw_request, payload)
    if CLIENT_PROBE_ENABLED:
        log.info("client-probe endpoint=chat client=%s fp=%s", _client_tag,
                 _fingerprint_downstream_client(raw_request, payload))

    if current_model in UNAVAILABLE_MODELS:
        log.warning(f"Rejecting known-unavailable model '{current_model}' without upstream attempts.")
        return unavailable_model_response(current_model)

    if REQ_DIAG_ENABLED:
        try:
            _dj = json.dumps(payload, ensure_ascii=False, default=str)
            _msg_list = payload.get("messages") if isinstance(payload.get("messages"), list) else []
            _last_user = ""
            for _m in reversed(_msg_list):
                if isinstance(_m, dict) and _m.get("role") == "user":
                    _c = _m.get("content")
                    if isinstance(_c, str):
                        _last_user = _c
                    elif isinstance(_c, list):
                        _last_user = " ".join(
                            p.get("text", "") for p in _c
                            if isinstance(p, dict) and isinstance(p.get("text"), str))
                    break
            log.info(
                "req-diag model=%s stream=%s msgs=%d imgs=%d files=%d tools=%d rf=%r last_user=%r body_len=%d",
                current_model, is_stream, len(_msg_list),
                _dj.count('"image_url"') + _dj.count('"input_image"'),
                _dj.count('"input_file"'),
                len(payload.get("tools")) if isinstance(payload.get("tools"), list) else 0,
                payload.get("response_format"), _last_user[:80], len(_dj),
            )
        except Exception:
            pass

    # 标题兜底预判（只记录、不拦截）：OpenCode 标题生成是无工具后台请求，
    # 偶发上游免费层 403。平时放行走上游以保证标题质量，仅在最终 403 时才
    # 本地合成返回，避免把 "[Upstream 403]..." 当标题显示。
    _tools_val = payload.get("tools")
    _has_real_tools = isinstance(_tools_val, list) and len(_tools_val) > 0
    _title_fallback: Optional[str] = None
    if not _has_real_tools and isinstance(payload.get("messages"), list):
        _title_fallback = _extract_local_title(payload.get("messages"))
        if _title_fallback is not None:
            log.info("title-request detected model=%s fallback=%r (passthrough upstream first)",
                     current_model, _title_fallback[:60])

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
        # response_format → text.format（上游不认 response_format，会 400）
        if "response_format" in payload:
            _rf_text = _response_format_to_text_format(payload.pop("response_format", None))
            if _rf_text is not None:
                _existing_text = payload.get("text")
                if isinstance(_existing_text, dict):
                    _existing_text = dict(_existing_text)
                    _existing_text["format"] = _rf_text["format"]
                    payload["text"] = _existing_text
                else:
                    payload["text"] = _rf_text
        # 移除 Responses API 不支持的 Chat Completions 参数
        for key in ("reasoning_effort", "n", "frequency_penalty", "presence_penalty", "stop", "user", "response_format"):
            payload.pop(key, None)
        _target_url = TARGET_ZEN_RESPONSES_URL

    # 免费层伪装：responses 形态补 responses-tools，否则补 chat-tools；
    # responses-only 免费模型的非流请求强制走上游流式再拼回 JSON。
    # _fake_tools_only：下游没带真工具时，上游看到的全是 shim 假 tools，
    # 模型若被诱发调用，下游执行不了，需由代理侧续一轮（见非流 collapse 处）。
    _fake_tools_only = (current_model in RESPONSES_ONLY_MODELS) and not _has_real_tools
    _downstream_tool_names = set()
    if isinstance(_tools_val, list):
        for _t in _tools_val:
            if isinstance(_t, dict):
                _fn = _t.get("function")
                _nm = (_fn.get("name") if isinstance(_fn, dict) else None) or _t.get("name")
                if _nm:
                    _downstream_tool_names.add(_nm)
    _trace_endpoint = "responses" if current_model in RESPONSES_ONLY_MODELS else "chat"
    if current_model in RESPONSES_ONLY_MODELS:
        payload = ensure_free_tier_tools_responses(payload)
    else:
        payload = ensure_free_tier_tools_chat(payload)
    force_upstream_stream = (
        (not is_stream) and FREE_TIER_SHIM_ENABLED
        and is_free_tier_model(current_model)
    )
    # thinking 模型回传注入（仅 chat 形态；responses-only 走 encrypted_content
    # 另一套协议，不适用）：客户端丢弃 reasoning_content 时按 tool_call id 从
    # 会话缓存补齐，严格上游不再 400。放 attempt 循环外只注入一次。
    if (current_model not in RESPONSES_ONLY_MODELS
            and isinstance(payload.get("messages"), list)):
        try:
            _inj = inject_reasoning_passback(payload, current_model)
            if _inj:
                log.info("reasoning passback injected into %d assistant msg(s) model=%s client=%s",
                         _inj, current_model, _client_tag)
        except Exception as exc:
            log.warning("reasoning passback injection failed: %s", exc)

    client_key = raw_request.headers.get("x-api-key", "") or raw_request.headers.get("authorization", "")
    headers = get_realistic_headers(client_key)
    for k, v in raw_request.headers.items():
        if k.lower().startswith("x-opencode-"):
            headers[k] = v
    headers = apply_downstream_auth(headers, raw_request)

    # 会话标识：优先用客户端传入的 x-opencode-session，否则用 client_key
    session_key = headers.get("x-opencode-session", "") or client_key

    if _title_fallback is not None and session_key and UPSTREAM_API_KEY_OVERRIDE.lower() == "public":
        # 新会话信号：标题请求只在 OpenCode 新会话首条消息时发一次，
        # 新会话换下一个出口（分摊日配额；small_model 直连时标题不经过
        # 代理，由下面的长间隔逻辑兜底）
        try:
            _cur = _session_proxy_get(session_key)
            _nxt = proxy_pool.next_proxy_after(_cur or proxy_pool.active_proxy(), model=current_model)
            if _nxt and _nxt != _cur:
                _session_proxy_set(session_key, _nxt)
                with _anon_egress_lock:
                    _anon_last_seen[session_key] = time.monotonic()
                log.info("New anonymous session (title request); egress %s -> %s.", _cur, _nxt)
        except Exception as exc:
            log.warning("Title-triggered egress advance failed: %s", exc)

    consecutive_timeouts = 0
    # 凭据级限流判定：本次请求的 429 轮换计数从零开始
    _reset_rate_limit_rotations()
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key, model=current_model)
            egress_key = await pace_egress_request(proxies)
            upstream_payload = payload
            if force_upstream_stream:
                upstream_payload = dict(payload)
                upstream_payload["stream"] = True
            if is_stream or force_upstream_stream:
                session, stream_pool_key, _ = _get_stream_session("chat", egress_key)
                stream_borrowed = True
                response = await _post_upstream_stream(
                    session, _target_url, json_body=upstream_payload, headers=headers,
                    proxies=proxies,
                    timeout=MODEL_TIMEOUT_OVERRIDES.get(current_model, STREAM_TIMEOUT),
                )
            else:
                session = _get_session("chat")
                response = await _post_upstream(
                    session, _target_url, json_body=payload, headers=headers,
                    proxies=proxies, timeout=120,
                )
            log_upstream_response(response, current_model, "chat_completions", attempt, proxies is not None)
            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=current_model).inc()
                category, _quota_retry_after, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    try:
                        proxy_pool.record_upstream_quota(_request_proxy_url(proxies), current_model, _quota_retry_after, "quota")
                    except Exception as exc:
                        log.warning("Record quota failed: %s", exc)
                    return upstream_rate_limit_response(response, current_model)
                if await recover_from_rate_limit(response, current_model, "chat", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None, session_key=session_key,
                                 current_proxy=_request_proxy_url(proxies) or "",
                                 rotation_used=_rate_limit_rotations.get() > 0):
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
                if (response.status_code == 403 and FREE_TIER_SHIM_ENABLED
                        and is_free_tier_model(current_model)
                        and attempt < MAX_RETRIES_ON_429):
                    # B方案（public匿名模式）：403 不轮换、不换 session、不解绑，
                    # 只做同出口指数退避。原因：同一 public key 短时间内从多
                    # 个机房 IP 跳变/并发，会被上游识别为代理共享并持续 403
                    #（FreeTierError），轮换反而坐实特征。保持会话粘性更像
                    # 真实单客户端重试。
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    else:
                        _reset_request_session("chat", session, pooled=not is_stream)
                    log.warning(
                        "Transient upstream 403 for '%s'; retrying same egress (%s/%s) egress=%s proxy=%s.",
                        current_model, attempt, MAX_RETRIES_ON_429, egress_key, _request_proxy_url(proxies),
                    )
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    await asyncio.sleep(delay)
                    continue
                log.warning("Upstream HTTP %s for '%s'; returning error to client.", response.status_code, current_model)
                if response.status_code == 400 and current_model in RESPONSES_ONLY_MODELS:
                    try:
                        log.warning("400-payload model=%s egress=%s keys=%s response_format=%r tool_choice=%r",
                                    current_model, egress_key, sorted(upstream_payload.keys()),
                                    upstream_payload.get("response_format"), upstream_payload.get("tool_choice"))
                    except Exception:
                        pass
                if response.status_code == 403 and current_model in RESPONSES_ONLY_MODELS:
                    try:
                        _dump = json.dumps(
                            {"tools": upstream_payload.get("tools"),
                             "tool_choice": upstream_payload.get("tool_choice"),
                             "input": upstream_payload.get("input"),
                             "stream": upstream_payload.get("stream"),
                             "model": upstream_payload.get("model")},
                            ensure_ascii=False, default=str,
                        )
                        log.warning("403-payload model=%s egress=%s len=%s body=%s", current_model, egress_key, len(_dump), _dump[:3000])
                    except Exception as _e:
                        log.warning("403-payload dump failed: %s", _e)
                if response.status_code == 403 and _title_fallback is not None:
                    # 标题兜底：仅当上游拒掉标题生成时，用本地合成代替
                    # “[Upstream 403]…” 错误文本，标题栏不再挂 403。平时放行，
                    # 99% 的标题仍由上游生成以保证质量；正常聊天不受影响。
                    log.warning("title-403-fallback model=%s title=%r", current_model, _title_fallback[:60])
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    try:
                        record_request_history(
                            model=current_model,
                            proxy_ip=_request_proxy_url(proxies) or "",
                            egress_ip=egress_key,
                            latency_ms=round((time.time() - start_time) * 1000, 1),
                            status="ok",
                            is_stream=bool(is_stream),
                            attempt=attempt,
                            error_msg="title_fallback",
                            client=_client_tag,
                            )
                    except Exception:
                        pass
                    track_token_usage(current_model, prompt_tokens=20, completion_tokens=5)
                    if is_stream:
                        _ftid = f"chatcmpl-{int(time.time())}"
                        _fnow = int(time.time())
                        _ftext = _title_fallback

                        async def _fallback_stream():
                            head = {"id": _ftid, "object": "chat.completion.chunk", "created": _fnow, "model": current_model,
                                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": _ftext}, "finish_reason": None}]}
                            yield f"data: {json.dumps(head, ensure_ascii=False, separators=(',', ':'))}\n\n"
                            yield "data: [DONE]\n\n"

                        return StreamingResponse(_fallback_stream(), media_type="text/event-stream",
                                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"})
                    return JSONResponse(content={
                        "id": f"chatcmpl-{int(time.time())}", "object": "chat.completion", "created": int(time.time()),
                        "model": current_model,
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": _title_fallback}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 20, "completion_tokens": 5, "total_tokens": 25},
                    })
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
                    error_sse = build_upstream_stream_error_sse(
                        response.status_code, error_body[:500], current_model)
                    record_request_history(
                        model=current_model,
                        proxy_ip=_request_proxy_url(proxies) or "",
                        egress_ip=egress_key,
                        latency_ms=round((time.time() - start_time) * 1000, 1),
                        status="failed",
                        is_stream=True,
                        attempt=attempt,
                        error_msg=f"upstream_{response.status_code}",
                        client=_client_tag,
                        )
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
            _reset_empty_stream_strikes(session_key)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=current_model).inc()
            prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)

            if is_stream:
                try:
                    # Pre-verify that the response is not an empty stream before committing to StreamingResponse
                    if current_model in RESPONSES_ONLY_MODELS:
                        _followup = None
                        if _fake_tools_only and isinstance(upstream_payload, dict):
                            # 下游无真工具：假 tools 诱发的调用下游执行不了，
                            # 由流函数代理侧续问（不播调用块）
                            _followup = {
                                "url": _target_url,
                                "headers": headers,
                                "proxies": proxies,
                                "input": upstream_payload.get("input"),
                                "tools": upstream_payload.get("tools"),
                                "timeout": MODEL_TIMEOUT_OVERRIDES.get(current_model, STREAM_TIMEOUT),
                            }
                        inner_gen = responses_stream_as_chat(response, current_model, session=session if stream_borrowed else None, stream_pool_key=stream_pool_key if stream_borrowed else None, followup=_followup)
                    else:
                        # 客户端未带 tools 时，网关注入的官方工具定义可能诱发上游调工具，
                        # 下游接不住会卡住 → 让流函数把 tool_calls 改写成文本并收尾为 stop。
                        inner_gen = stream_response(
                            response, current_model, session=session,
                            stream_pool_key=stream_pool_key if stream_borrowed else None,
                            suppress_tool_calls=(
                                (not _has_real_tools) and is_free_tier_model(current_model)
                            ),
                        )
                    stream_gen = _stream_with_history(
                        inner_gen, model=current_model, proxies=proxies,
                        egress_key=egress_key, start_time=start_time, attempt=attempt,
                        endpoint=_trace_endpoint, client_tools=_has_real_tools,
                        upstream_status=getattr(response, "status_code", None),
                        client=_client_tag,
                    )
                    # 流式只记一条 history（包装器播完才写）；真实 token 由各流函数
                    # 结束时补记（count_request=False，不重数），这里只计请求数。
                    track_token_usage(current_model, prompt_tokens=0, completion_tokens=0)
                    return StreamingResponse(
                        stream_gen,
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
                    )
                except EmptyStreamError:
                    log.warning("Empty stream for '%s'; retrying (%s/%s).", current_model, attempt, MAX_RETRIES_ON_429)
                    _note_empty_stream(session_key, current_model)
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    await asyncio.sleep(delay)
                    continue
            else:
                with FlowContext():
                    try:
                        if force_upstream_stream:
                            if current_model in RESPONSES_ONLY_MODELS:
                                res_json = await _collect_responses_stream(response)
                            else:
                                res_json = await _collect_chat_stream(response, current_model)
                            if (current_model in RESPONSES_ONLY_MODELS and _fake_tools_only
                                    and isinstance(res_json, dict)):
                                # 下游无真工具时模型被 shim 假 tools 诱发的调用下游执行不了：
                                # 代理侧合成工具结果续问一轮，否则下游只见前言文本就停住
                                _round1_calls = [it for it in (res_json.get("output") or [])
                                                 if isinstance(it, dict) and it.get("type") == "function_call"]
                                _unexecutable = [c for c in _round1_calls
                                                 if (c.get("name") or "") not in _downstream_tool_names]
                                if _unexecutable:
                                    log.info("Fake-tool calls %s on '%s'; continuing proxy-side (1 round).",
                                             [c.get("name") for c in _unexecutable], current_model)
                                    _follow_input = list((upstream_payload.get("input") if isinstance(upstream_payload, dict) else []) or [])
                                    _follow_input = _follow_input + _round1_calls + [
                                        {"type": "function_call_output",
                                         "call_id": c.get("call_id") or c.get("id") or "call_unknown",
                                         "output": _FAKE_TOOL_CONTINUE_OUTPUT}
                                        for c in _round1_calls]
                                    _payload2 = dict(upstream_payload) if isinstance(upstream_payload, dict) else {"model": current_model}
                                    _payload2["input"] = _follow_input
                                    try:
                                        _resp2 = await _post_upstream_stream(
                                            session, _target_url, json_body=_payload2, headers=headers,
                                            proxies=proxies,
                                            timeout=MODEL_TIMEOUT_OVERRIDES.get(current_model, STREAM_TIMEOUT),
                                        )
                                        res_json = await _collect_responses_stream(_resp2)
                                        # 只续一轮：若仍调工具则剥离调用返回文本，保证终止
                                        _out2 = res_json.get("output") if isinstance(res_json, dict) else None
                                        if isinstance(_out2, list) and any(isinstance(it, dict) and it.get("type") == "function_call" for it in _out2):
                                            res_json["output"] = [it for it in _out2 if not (isinstance(it, dict) and it.get("type") == "function_call")]
                                    except EmptyStreamError:
                                        pass
                            _release_stream_session(stream_pool_key, session, discard=False)
                            stream_borrowed = False
                        else:
                            res_json = await asyncio.to_thread(response.json)
                        # Responses-only 模型：将 Responses API 响应转为 Chat Completions 格式
                        if current_model in RESPONSES_ONLY_MODELS:
                            res_json = _convert_responses_to_chat(res_json, current_model)
                        usage = res_json.get("usage", {})
                        # chat 路径 usage 可能是 prompt_tokens（chat 上游）或
                        # input_tokens（responses 上游经 _convert_responses_to_chat 透传）
                        track_token_usage(
                            current_model,
                            prompt_tokens=_pick_usage_num(usage, "prompt_tokens", "input_tokens", default=DEFAULT_PROMPT_TOKENS),
                            completion_tokens=_pick_usage_num(usage, "completion_tokens", "output_tokens", default=DEFAULT_COMPLETION_TOKENS)
                        )
                        record_request_history(
                            model=current_model,
                            proxy_ip=_request_proxy_url(proxies) or "",
                            egress_ip=egress_key,
                            latency_ms=round((time.time() - start_time) * 1000, 1),
                            status="ok",
                            is_stream=False,
                            attempt=attempt,
                            client=_client_tag,
                            )
                        try:
                            record_request_trace(
                                model=current_model,
                                endpoint=_trace_endpoint,
                                is_stream=False,
                                client_tools=_has_real_tools,
                                proxy_ip=_request_proxy_url(proxies) or "",
                                egress_ip=egress_key,
                                ttft_ms=None,
                                total_ms=(time.time() - start_time) * 1000,
                                chunks=1,
                                out_bytes=len(json.dumps(res_json, ensure_ascii=False)),
                                status="ok",
                                attempt=attempt,
                                finish_reason=str((res_json.get("choices") or [{}])[0].get("finish_reason") or ""),
                                downstream_tool_calls=bool((res_json.get("choices") or [{}])[0].get("message", {}).get("tool_calls")),
                                upstream_status=getattr(response, "status_code", None),
                                prompt_tokens=_pick_usage_num(usage, "prompt_tokens", "input_tokens", default=0),
                                completion_tokens=_pick_usage_num(usage, "completion_tokens", "output_tokens", default=0),
                                client=_client_tag,
                                )
                        except Exception:
                            pass
                        return JSONResponse(content=res_json)
                    except EmptyStreamError:
                        if stream_borrowed:
                            _release_stream_session(stream_pool_key, session, discard=True)
                            stream_borrowed = False
                        log.warning("Empty forced stream for '%s'; retrying (%s/%s).", current_model, attempt, MAX_RETRIES_ON_429)
                        _note_empty_stream(session_key, current_model)
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
        client=_client_tag,
        )
    try:
        record_request_trace(
            model=current_model,
            endpoint=_trace_endpoint,
            is_stream=bool(is_stream),
            client_tools=_has_real_tools,
            proxy_ip=_request_proxy_url(proxies) if proxies else "",
            egress_ip=egress_key,
            ttft_ms=None,
            total_ms=(time.time() - start_time) * 1000,
            chunks=0,
            out_bytes=0,
            status="failed",
            attempt=MAX_RETRIES_ON_429,
            finish_reason="",
            downstream_tool_calls=False,
            upstream_status=getattr(response, "status_code", None),
            error_type="exhausted",
            client=_client_tag,
            )
    except Exception:
        pass
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
    _client_tag = detect_client(raw_request, body)
    if CLIENT_PROBE_ENABLED:
        log.info("client-probe endpoint=anthropic client=%s fp=%s", _client_tag,
                 _fingerprint_downstream_client(raw_request, body))

    if model_name in UNAVAILABLE_MODELS:
        log.warning(f"Rejecting known-unavailable model '{model_name}' without upstream attempts.")
        return unavailable_model_response(model_name)

    body = ensure_free_tier_tools_anthropic(body)
    force_upstream_stream = (
        (not is_stream) and FREE_TIER_SHIM_ENABLED
        and is_free_tier_model(model_name)
    )

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
    headers = apply_downstream_auth(headers, raw_request)

    # 会话标识：优先用客户端传入的 x-opencode-session
    session_key = headers.get("x-opencode-session", "") or client_api_key

    consecutive_timeouts = 0
    # 凭据级限流判定：本次请求的 429 轮换计数从零开始
    _reset_rate_limit_rotations()
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key, model=model_name)
            egress_key = await pace_egress_request(proxies)
            upstream_body = body
            if force_upstream_stream:
                upstream_body = dict(body)
                upstream_body["stream"] = True
            if is_stream or force_upstream_stream:
                session, stream_pool_key, _ = _get_stream_session("anthropic", egress_key)
                stream_borrowed = True
                response = await _post_upstream_stream(
                    session, TARGET_ZEN_ANTHROPIC_URL, json_body=upstream_body, headers=headers,
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
                category, _quota_retry_after, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    try:
                        proxy_pool.record_upstream_quota(_request_proxy_url(proxies), model_name, _quota_retry_after, "quota")
                    except Exception as exc:
                        log.warning("Record quota failed: %s", exc)
                    return upstream_rate_limit_response(response, model_name)
                if await recover_from_rate_limit(response, model_name, "anthropic", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None, session_key=session_key,
                                 current_proxy=_request_proxy_url(proxies) or "",
                                 rotation_used=_rate_limit_rotations.get() > 0):
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
            _reset_empty_stream_strikes(session_key)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="anthropic_messages").observe(time.time() - start_time)

            if is_stream:
                stream_gen = _stream_with_history(
                    stream_response(response, model_name, session=session, protocol="anthropic", stream_pool_key=stream_pool_key if stream_borrowed else None),
                    model=model_name, proxies=proxies,
                    egress_key=egress_key, start_time=start_time, attempt=attempt,
                    endpoint="anthropic", client_tools=bool(body.get("tools")),
                    upstream_status=getattr(response, "status_code", None),
                    client=_client_tag,
                )
                track_token_usage(model_name, prompt_tokens=0, completion_tokens=0)
                return StreamingResponse(
                    stream_gen,
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        if force_upstream_stream:
                            res_json = await _collect_anthropic_stream(response, model_name)
                            _release_stream_session(stream_pool_key, session, discard=False)
                            stream_borrowed = False
                        else:
                            res_json = await asyncio.to_thread(response.json)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            model_name,
                            prompt_tokens=_pick_usage_num(usage, "input_tokens", "prompt_tokens", default=DEFAULT_PROMPT_TOKENS),
                            completion_tokens=_pick_usage_num(usage, "output_tokens", "completion_tokens", default=DEFAULT_COMPLETION_TOKENS),
                        )
                        record_request_history(
                            model=model_name,
                            proxy_ip=_request_proxy_url(proxies) or "",
                            egress_ip=egress_key,
                            latency_ms=round((time.time() - start_time) * 1000, 1),
                            status="ok",
                            is_stream=False,
                            attempt=attempt,
                            client=_client_tag,
                            )
                        try:
                            _an_tc = any(
                                isinstance(c, dict) and c.get("type") == "tool_use"
                                for c in (res_json.get("content") or [])
                            ) if isinstance(res_json, dict) else False
                            record_request_trace(
                                model=model_name,
                                endpoint="anthropic",
                                is_stream=False,
                                client_tools=bool(body.get("tools")),
                                proxy_ip=_request_proxy_url(proxies) or "",
                                egress_ip=egress_key,
                                ttft_ms=None,
                                total_ms=(time.time() - start_time) * 1000,
                                chunks=1,
                                out_bytes=len(json.dumps(res_json, ensure_ascii=False)),
                                status="ok",
                                attempt=attempt,
                                finish_reason=str(res_json.get("stop_reason") or ""),
                                downstream_tool_calls=_an_tc,
                                upstream_status=getattr(response, "status_code", None),
                                prompt_tokens=_pick_usage_num(usage, "input_tokens", "prompt_tokens", default=0),
                                completion_tokens=_pick_usage_num(usage, "output_tokens", "completion_tokens", default=0),
                                client=_client_tag,
                                )
                        except Exception:
                            pass
                        return JSONResponse(content=res_json)
                    except EmptyStreamError:
                        if stream_borrowed:
                            _release_stream_session(stream_pool_key, session, discard=True)
                            stream_borrowed = False
                        log.warning("Empty forced anthropic stream for '%s'; retrying (%s/%s).", model_name, attempt, MAX_RETRIES_ON_429)
                        _note_empty_stream(session_key, model_name)
                        delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                        await asyncio.sleep(delay)
                        continue
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
    _client_tag = detect_client(raw_request, body)
    if CLIENT_PROBE_ENABLED:
        log.info("client-probe endpoint=responses client=%s fp=%s", _client_tag,
                 _fingerprint_downstream_client(raw_request, body))

    if model_name in UNAVAILABLE_MODELS:
        log.warning(f"Rejecting known-unavailable model '{model_name}' without upstream attempts.")
        return unavailable_model_response(model_name)

    client_key = raw_request.headers.get("x-api-key", "") or raw_request.headers.get("authorization", "")
    headers = get_realistic_headers(client_key)
    for k, v in raw_request.headers.items():
        if k.lower().startswith("x-opencode-"):
            headers[k] = v
    headers = apply_downstream_auth(headers, raw_request)
    body = ensure_free_tier_tools_responses(body)
    force_upstream_stream = (
        (not is_stream) and FREE_TIER_SHIM_ENABLED and is_free_tier_model(model_name)
    )

    # 会话标识：优先用客户端传入的 x-opencode-session
    session_key = headers.get("x-opencode-session", "") or client_key

    consecutive_timeouts = 0
    # 凭据级限流判定：本次请求的 429 轮换计数从零开始
    _reset_rate_limit_rotations()
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        session = None
        stream_pool_key = None
        stream_borrowed = False
        proxies = None
        egress_key = "unknown"
        try:
            proxies = get_next_outbound_proxy(session_key=session_key, model=model_name)
            egress_key = await pace_egress_request(proxies)
            upstream_body = body
            if force_upstream_stream:
                upstream_body = dict(body)
                upstream_body["stream"] = True
            if is_stream or force_upstream_stream:
                session, stream_pool_key, _ = _get_stream_session("responses", egress_key)
                stream_borrowed = True
                response = await _post_upstream_stream(
                    session, TARGET_ZEN_RESPONSES_URL, json_body=upstream_body, headers=headers,
                    proxies=proxies,
                    timeout=MODEL_TIMEOUT_OVERRIDES.get(model_name, STREAM_TIMEOUT),
                )
            else:
                session = _get_session("responses")
                response = await _post_upstream(
                    session, TARGET_ZEN_RESPONSES_URL, json_body=body, headers=headers,
                    proxies=proxies, timeout=120,
                )
            log_upstream_response(response, model_name, "responses", attempt, proxies is not None)
            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                category, _quota_retry_after, _ = classify_upstream_429(response)
                if category == "quota":
                    if stream_borrowed:
                        _release_stream_session(stream_pool_key, session, discard=True)
                        stream_borrowed = False
                    try:
                        proxy_pool.record_upstream_quota(_request_proxy_url(proxies), model_name, _quota_retry_after, "quota")
                    except Exception as exc:
                        log.warning("Record quota failed: %s", exc)
                    return upstream_rate_limit_response(response, model_name)
                if await recover_from_rate_limit(response, model_name, "responses", session, not is_stream, egress_key, attempt, stream_pool_key=stream_pool_key if stream_borrowed else None, session_key=session_key,
                                 current_proxy=_request_proxy_url(proxies) or "",
                                 rotation_used=_rate_limit_rotations.get() > 0):
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
            _reset_empty_stream_strikes(session_key)
            _egress_rate_policy.record_success(egress_key)
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="responses").observe(time.time() - start_time)

            if is_stream:
                stream_gen = _stream_with_history(
                    stream_response(response, model_name, session=session, protocol="responses", stream_pool_key=stream_pool_key if stream_borrowed else None),
                    model=model_name, proxies=proxies,
                    egress_key=egress_key, start_time=start_time, attempt=attempt,
                    endpoint="responses", client_tools=bool(body.get("tools")),
                    upstream_status=getattr(response, "status_code", None),
                    client=_client_tag,
                )
                track_token_usage(model_name, prompt_tokens=0, completion_tokens=0)
                return StreamingResponse(
                    stream_gen,
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        if force_upstream_stream:
                            res_json = await _collect_responses_stream(response)
                            _release_stream_session(stream_pool_key, session, discard=False)
                            stream_borrowed = False
                        else:
                            res_json = await asyncio.to_thread(response.json)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            model_name,
                            prompt_tokens=_pick_usage_num(usage, "input_tokens", "prompt_tokens", default=DEFAULT_PROMPT_TOKENS),
                            completion_tokens=_pick_usage_num(usage, "output_tokens", "completion_tokens", default=DEFAULT_COMPLETION_TOKENS),
                        )
                        record_request_history(
                            model=model_name,
                            proxy_ip=_request_proxy_url(proxies) or "",
                            egress_ip=egress_key,
                            latency_ms=round((time.time() - start_time) * 1000, 1),
                            status="ok",
                            is_stream=False,
                            attempt=attempt,
                            client=_client_tag,
                            )
                        try:
                            _rp_fc = ""
                            try:
                                _rp_fc = str((res_json.get("output") or [{}])[0].get("status") or "")
                            except Exception:
                                _rp_fc = ""
                            record_request_trace(
                                model=model_name,
                                endpoint="responses",
                                is_stream=False,
                                client_tools=bool(body.get("tools")),
                                proxy_ip=_request_proxy_url(proxies) or "",
                                egress_ip=egress_key,
                                ttft_ms=None,
                                total_ms=(time.time() - start_time) * 1000,
                                chunks=1,
                                out_bytes=len(json.dumps(res_json, ensure_ascii=False)),
                                status="ok",
                                attempt=attempt,
                                finish_reason=_rp_fc,
                                downstream_tool_calls=any(
                                    isinstance(o, dict) and o.get("type") == "function_call"
                                    for o in (res_json.get("output") or [])
                                ) if isinstance(res_json, dict) else False,
                                upstream_status=getattr(response, "status_code", None),
                                prompt_tokens=_pick_usage_num(usage, "input_tokens", "prompt_tokens", default=0),
                                completion_tokens=_pick_usage_num(usage, "output_tokens", "completion_tokens", default=0),
                                client=_client_tag,
                            )
                        except Exception:
                            pass
                        return JSONResponse(content=res_json)
                    except EmptyStreamError:
                        if stream_borrowed:
                            _release_stream_session(stream_pool_key, session, discard=True)
                            stream_borrowed = False
                        log.warning("Empty forced responses stream for '%s'; retrying (%s/%s).", model_name, attempt, MAX_RETRIES_ON_429)
                        _note_empty_stream(session_key, model_name)
                        delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                        await asyncio.sleep(delay)
                        continue
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
