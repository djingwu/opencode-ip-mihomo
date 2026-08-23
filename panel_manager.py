#!/usr/bin/env python3
"""面板管理后端：订阅/节点/组管理 + mihomo 热重载。

所有操作通过改写共享的 mihomo config.yaml（./mihomo/config.yaml）后调用
mihomo external-controller API (PUT /configs?force=true) 热重载实现，
无需重启容器。
"""
import base64
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
import re
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote
from typing import Any, Callable, Dict, List, Optional

from curl_cffi import requests
from proxy_utils import normalize_proxy_url
import proxy_pool

log = logging.getLogger("panel")

# -----------------------------------------------------------------------------
# 环境配置
# -----------------------------------------------------------------------------
MIHOMO_API_URL = os.environ.get("MIHOMO_API_URL", "http://mihomo:9090").rstrip("/")
MIHOMO_SECRET = os.environ.get("MIHOMO_SECRET", "").strip()
# 这是 mihomo 容器内的路径，不是 proxy-server 的 /data/mihomo 路径。
MIHOMO_CONTROLLER_CONFIG_PATH = os.environ.get("MIHOMO_CONTROLLER_CONFIG_PATH", "/root/.config/mihomo/config.yaml")
# 共享配置目录（compose 挂载 ./mihomo:/data/mihomo）
MIHOMO_CONFIG_DIR = Path(os.environ.get("MIHOMO_CONFIG_DIR", "/data/mihomo"))
CONFIG_FILE = MIHOMO_CONFIG_DIR / "config.yaml"
# 面板持久化状态（节点勾选、自定义组）
PANEL_STATE_FILE = MIHOMO_CONFIG_DIR / "panel_state.json"
# 延迟测试配置
# 注意：面板测速必须由 mihomo 控制器对每个指定节点直接探测，不能
# 复用当前 ROTATOR 出口，否则加入/应用其它订阅后，免费节点可能被组级
# 健康检查结果覆盖为“未检测/超时”。
LATENCY_TIMEOUT_MS = max(1000, int(os.environ.get("PANEL_LATENCY_TIMEOUT", "15000")))
LATENCY_ATTEMPTS = max(1, int(os.environ.get("PANEL_LATENCY_ATTEMPTS", "2")))
LATENCY_RETRY_DELAY = max(0.0, float(os.environ.get("PANEL_LATENCY_RETRY_DELAY", "0.25")))
LATENCY_WORKERS = max(1, int(os.environ.get("PANEL_LATENCY_WORKERS", "8")))
# mihomo 的 delay API 会为每个节点建立独立连接。500+ 节点一次性同时提交
# 会把控制器和 VPS 的连接池打满，因此按小批次排队，保证整批任务最终汇总。
LATENCY_BATCH_SIZE = max(1, int(os.environ.get("PANEL_LATENCY_BATCH_SIZE", "40")))
LATENCY_BATCH_PAUSE = max(0.0, float(os.environ.get("PANEL_LATENCY_BATCH_PAUSE", "0.15")))
MIHOMO_OUTBOUND_PROXY = os.environ.get("CUSTOM_OUTBOUND_PROXY", "http://mihomo:7890").strip()

_lock = threading.Lock()
_latency_probe_lock = threading.Lock()
_extract_job_lock = threading.Lock()
_extract_jobs: Dict[str, Dict[str, Any]] = {}
EXTRACT_JOB_TTL = max(300, int(os.environ.get("PANEL_EXTRACT_JOB_TTL", "7200")))
_delay_job_lock = threading.Lock()
_delay_jobs: Dict[str, Dict[str, Any]] = {}
DELAY_JOB_TTL = max(300, int(os.environ.get("PANEL_DELAY_JOB_TTL", "7200")))

DEFAULT_UA = "clash-verge/v2.0.0"
HEALTH_CHECK_URL = "https://www.gstatic.com/generate_204"

# -----------------------------------------------------------------------------
# mihomo external-controller API
# -----------------------------------------------------------------------------
def _headers() -> Dict[str, str]:
    h = {"User-Agent": "opencode-ip-mihomo-panel"}
    if MIHOMO_SECRET:
        h["Authorization"] = f"Bearer {MIHOMO_SECRET}"
    return h


def mihomo_get(path: str) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(f"{MIHOMO_API_URL}{path}", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
        log.debug("mihomo GET %s -> HTTP %s", path, resp.status_code)
    except Exception as e:
        log.debug("mihomo GET %s failed: %s", path, e)
    return None


def mihomo_put(path: str, payload: Optional[Dict[str, Any]] = None) -> bool:
    try:
        resp = requests.put(
            f"{MIHOMO_API_URL}{path}",
            json=payload,
            headers=_headers(),
            timeout=15,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        log.debug("mihomo PUT %s failed: %s", path, e)
    return False


def mihomo_reload() -> bool:
    """通过 mihomo Controller 热重载配置，兼容需要 path payload 的版本。"""
    if not MIHOMO_API_URL:
        return False
    endpoint = f"{MIHOMO_API_URL}/configs?force=true"
    attempts = [
        {"path": MIHOMO_CONTROLLER_CONFIG_PATH},
        None,
    ]
    for payload in attempts:
        try:
            resp = requests.put(endpoint, json=payload, headers=_headers(), timeout=20)
            if resp.status_code in (200, 204):
                return True
            log.warning("mihomo config reload returned HTTP %s: %s", resp.status_code, resp.text[:300])
        except Exception as exc:
            log.warning("mihomo config reload failed: %s", exc)
    return False


def _sanitize_free_provider_cache(provider: Dict[str, Any]) -> List[str]:
    """Remove local/private literal endpoints from an existing free provider cache."""
    path = _resolve_provider_file(provider)
    if not path or not path.exists() or not path.is_file():
        return []
    try:
        import yaml
        from free_nodes import is_routable_proxy_server

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        proxies = data.get("proxies") if isinstance(data, dict) else None
        if not isinstance(proxies, list):
            return []
        removed = [
            str(proxy.get("name") or "")
            for proxy in proxies
            if isinstance(proxy, dict) and not is_routable_proxy_server(proxy.get("server"))
        ]
        if not removed:
            return []
        data["proxies"] = [
            proxy
            for proxy in proxies
            if not isinstance(proxy, dict) or is_routable_proxy_server(proxy.get("server"))
        ]
        path.write_text(
            yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
        log.warning("removed %d unroutable endpoints from free provider cache: %s", len(removed), removed[:5])
        return removed
    except Exception as exc:
        log.warning("sanitize free provider cache failed: %s", exc)
        return []


def normalize_mihomo_config() -> Dict[str, Any]:
    """Repair legacy provider fields before/after a mihomo restart.

    Older builds wrote ``header: {User-Agent: ...}``, but current mihomo expects
    a different header shape and aborts startup. Removing the optional header is
    safe because ``user-agent`` is already supported by the provider config.
    """
    with _lock:
        cfg = load_config() or {}
        providers = cfg.get("proxy-providers", {}) or {}
        changed = False
        repaired: List[str] = []
        for name, provider in providers.items():
            if not isinstance(provider, dict):
                continue
            if str(name) == "free":
                removed = _sanitize_free_provider_cache(provider)
                if removed:
                    changed = True
                    repaired.append("free-cache")
            header = provider.get("header")
            if isinstance(header, dict):
                provider.pop("header", None)
                changed = True
                repaired.append(str(name))
            if provider.get("type") == "file":
                for field in ("url", "interval", "user-agent"):
                    if field in provider:
                        provider.pop(field, None)
                        changed = True
                        if str(name) not in repaired:
                            repaired.append(str(name))
        if _ensure_rotator_group_sources(cfg):
            changed = True
            repaired.append("ROTATOR")
        if changed and not save_config(cfg):
            return {"ok": False, "changed": False, "repaired": repaired, "error": "配置修复写入失败"}
    return {"ok": True, "changed": changed, "repaired": list(dict.fromkeys(repaired))}


def mihomo_refresh_providers() -> bool:
    """兼容旧调用：触发 mihomo 原生 provider 刷新。"""
    return mihomo_put("/providers/proxies")


def mihomo_provider_healthcheck(name: str) -> bool:
    """请求 mihomo 立即检测一个 file provider 的节点。

    provider 的定时 health-check 不一定会在热重载后马上执行；主动触发后，
    节点列表会逐步获得 alive/history/delay，而不是长期显示“未检测”。
    """
    provider_name = quote(str(name), safe="")
    try:
        resp = requests.put(
            f"{MIHOMO_API_URL}/providers/proxies/{provider_name}/healthcheck",
            headers=_headers(),
            timeout=20,
        )
        return resp.status_code in (200, 204)
    except Exception as exc:
        log.debug("mihomo provider healthcheck %s failed: %s", name, exc)
        return False


def warmup_provider_healthchecks(attempts: int = 30, delay: float = 1.0) -> Dict[str, Any]:
    """等待 mihomo/provider 就绪后主动触发 provider 健康检查。

    file provider（尤其是 free）在容器重建后没有 mihomo 内存中的历史延迟。
    后台预热会等待 provider 真正出现在 Controller，再逐个触发 healthcheck；
    控制器或 provider 尚未就绪时只重试当前任务，不影响 Web 服务启动。
    """
    cfg = load_config() or {}
    providers = cfg.get("proxy-providers", {}) or {}
    targets = [
        str(name)
        for name, provider in providers.items()
        if isinstance(provider, dict)
        and isinstance(provider.get("health-check"), dict)
        and provider["health-check"].get("enable", True) is not False
    ]
    if not targets:
        return {"ok": True, "triggered": [], "attempts": 0}

    pending = set(targets)
    triggered: List[str] = []
    total_attempts = max(1, int(attempts))
    for attempt in range(1, total_attempts + 1):
        live = mihomo_get("/providers/proxies")
        available: set[str] = set()
        if isinstance(live, dict):
            raw = live.get("providers", {})
            if isinstance(raw, dict):
                available = {str(name) for name in raw.keys()}
            elif isinstance(raw, list):
                available = {
                    str(item.get("name"))
                    for item in raw
                    if isinstance(item, dict) and str(item.get("name") or "").strip()
                }
        for name in list(pending.intersection(available)):
            if mihomo_provider_healthcheck(name):
                pending.discard(name)
                triggered.append(name)
        if not pending:
            return {
                "ok": True,
                "triggered": triggered,
                "targets": targets,
                "attempts": attempt,
            }
        if attempt < total_attempts and delay > 0:
            time.sleep(delay)
    log.warning(
        "mihomo provider healthcheck warmup timed out after %d attempts; pending=%s",
        total_attempts,
        sorted(pending),
    )
    return {
        "ok": False,
        "triggered": triggered,
        "targets": targets,
        "pending": sorted(pending),
        "attempts": total_attempts,
    }


def _decode_subscription_payload(content: bytes) -> tuple[str, str]:
    """识别 Clash YAML/JSON、明文 URI 列表和 base64 URI 列表。

    不同机场返回格式差异很大。先在 proxy-server 中统一落盘为 mihomo
    file provider，避免控制器对 base64 订阅格式判断不一致导致节点数为 0。
    """
    raw = content.decode("utf-8", errors="replace").lstrip("\ufeff").strip()
    if not raw:
        raise ValueError("订阅响应为空")

    try:
        import yaml
        parsed = yaml.safe_load(raw)
        if isinstance(parsed, dict) and isinstance(parsed.get("proxies"), list):
            return yaml.safe_dump(parsed, allow_unicode=True, sort_keys=False, default_flow_style=False), "yaml"
    except Exception:
        pass

    def looks_like_uri_list(value: str) -> bool:
        return any(line.strip().lower().startswith(("vmess://", "vless://", "trojan://", "ss://", "hysteria2://", "hy2://", "anytls://")) for line in value.splitlines())

    def uri_text_to_yaml(value: str) -> Optional[str]:
        try:
            from free_nodes import parse_uri, to_clash_yaml
            nodes = []
            for line in value.splitlines():
                line = line.strip()
                if not line:
                    continue
                node = parse_uri(line)
                if node:
                    nodes.append(node)
            if nodes:
                return to_clash_yaml(nodes) + "\n"
        except Exception as exc:
            log.debug("normalize URI subscription failed: %s", exc)
        return None

    if looks_like_uri_list(raw):
        normalized = uri_text_to_yaml(raw)
        if normalized:
            return normalized, "yaml"
        raise ValueError("订阅 URI 中没有可识别的节点")

    compact = "".join(line.strip() for line in raw.splitlines())
    try:
        decoded = base64.b64decode(compact + "=" * (-len(compact) % 4)).decode("utf-8", errors="replace").strip()
    except Exception as exc:
        raise ValueError("无法识别订阅格式（不是 Clash YAML，也不是 URI/base64 订阅）") from exc
    if looks_like_uri_list(decoded):
        normalized = uri_text_to_yaml(decoded)
        if normalized:
            return normalized, "yaml"
        raise ValueError("base64 订阅 URI 中没有可识别的节点")
    raise ValueError("订阅内容未包含可识别的节点列表")


def _subscription_source_url(name: str, provider: Dict[str, Any], state: Dict[str, Any]) -> str:
    sources = state.get("subscription_urls", {}) if isinstance(state.get("subscription_urls", {}), dict) else {}
    return str(provider.get("url") or sources.get(name) or "").strip()


def _fetch_and_cache_subscription(name: str, provider: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    url = _subscription_source_url(name, provider, state)
    if not url.startswith(("http://", "https://")):
        return {"name": name, "ok": False, "error": "缺少有效订阅链接"}
    try:
        response = requests.get(
            url,
            headers={"User-Agent": DEFAULT_UA, "Accept": "*/*"},
            timeout=30,
            impersonate="chrome124",
        )
        if response.status_code != 200:
            return {"name": name, "ok": False, "error": f"订阅返回 HTTP {response.status_code}"}
        normalized, fmt = _decode_subscription_payload(response.content)
        path = _resolve_provider_file(provider)
        if not path:
            path = MIHOMO_CONFIG_DIR / "providers" / f"{name}.yaml"
            provider["path"] = f"./providers/{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized, encoding="utf-8")
        # file provider 不要继续保留 http provider 的 url/header/interval 字段，
        # 否则部分 mihomo 版本会在 PUT /configs 热重载时拒绝整份配置。
        health_check = provider.get("health-check") or {
            "enable": True,
            "interval": 300,
            "url": HEALTH_CHECK_URL,
        }
        provider.clear()
        provider.update({
            "type": "file",
            "path": f"./providers/{name}.yaml",
            "health-check": health_check,
        })
        state.setdefault("subscription_urls", {})[name] = url
        state.setdefault("subscription_meta", {})[name] = {"format": fmt, "updated_at": int(time.time())}
        return {"name": name, "ok": True, "format": fmt, "url": url, "path": str(path)}
    except Exception as exc:
        log.warning("subscription %s refresh failed: %s", name, exc)
        return {"name": name, "ok": False, "error": str(exc)}


def refresh_subscriptions(names: Optional[List[str]] = None) -> Dict[str, Any]:
    """手动抓取并缓存订阅，再热重载 mihomo。"""
    with _lock:
        cfg = load_config() or {}
        providers = cfg.get("proxy-providers", {}) or {}
        state = load_state()
        targets = [name for name in (names or list(providers.keys())) if name in providers and name != FREE_PROVIDER_NAME]
        results = [_fetch_and_cache_subscription(name, providers[name], state) for name in targets if isinstance(providers[name], dict)]
        if not save_config(cfg):
            return {"ok": False, "error": "配置写入失败", "results": results}
        # “删除节点”只影响当前节点管理视图；手动刷新订阅/节点列表后，
        # provider 中仍存在的节点应重新出现。尤其是 free provider，它是
        # 本地文件，不参与远端抓取，但也必须在这里恢复可见性。
        excluded = state.get("excluded_nodes", [])
        if isinstance(excluded, list):
            cached_names = set()
            for provider in providers.values():
                if isinstance(provider, dict):
                    cached_names.update(_provider_cache_node_names(provider))
            if cached_names:
                state["excluded_nodes"] = [name for name in excluded if name not in cached_names]
        save_state(state)
    reload_ok = mihomo_reload()
    if reload_ok and FREE_PROVIDER_NAME in providers:
        mihomo_provider_healthcheck(FREE_PROVIDER_NAME)
    ok_count = sum(1 for result in results if result.get("ok"))
    return {"ok": bool(reload_ok and all(result.get("ok") for result in results)) if results else bool(reload_ok), "reload_ok": reload_ok, "total": len(results), "ok_count": ok_count, "results": results}


def _normalize_delay(value: Any) -> int:
    try:
        delay = int(value)
        return delay if delay > 0 else -1
    except (TypeError, ValueError):
        return -1


def _mihomo_group_delays(group: str = "ROTATOR") -> Dict[str, int]:
    try:
        resp = requests.get(
            f"{MIHOMO_API_URL}/group/{quote(group, safe='')}/delay",
            params={"url": HEALTH_CHECK_URL, "timeout": LATENCY_TIMEOUT_MS},
            headers=_headers(),
            timeout=LATENCY_TIMEOUT_MS / 1000 + 10,
        )
        if resp.status_code == 200:
            data = resp.json() or {}
            return {str(name): _normalize_delay(delay) for name, delay in data.items()}
        log.warning("group delay test returned HTTP %s: %s", resp.status_code, resp.text[:300])
    except Exception as exc:
        log.warning("group delay test failed: %s", exc)
    return {}


def _mihomo_single_delay(name: str) -> int:
    """通过 mihomo 对 *name* 指向的节点做独立延迟探测。

    这里故意不请求 ``/group/ROTATOR/delay``，也不通过
    ``MIHOMO_OUTBOUND_PROXY`` 发请求。mihomo 的 ``/proxies/:name/delay``
    会在 mihomo 容器内直接使用指定节点建立连接，因此不会因为当前
    ROTATOR 组切到了用户订阅节点而“借用”该节点测量免费节点。

    免费公共节点首包握手经常较慢；每个中性地址允许少量重试，避免一次
    短暂的连接竞争就把节点判为失效。
    """
    node_name = str(name or "").strip()
    if not node_name:
        return -1
    probe_urls = list(dict.fromkeys([HEALTH_CHECK_URL, "https://cp.cloudflare.com/generate_204"]))
    endpoint = f"{MIHOMO_API_URL}/proxies/{quote(node_name, safe='')}/delay"
    for attempt in range(LATENCY_ATTEMPTS):
        for probe_url in probe_urls:
            try:
                resp = requests.get(
                    endpoint,
                    params={"url": probe_url, "timeout": LATENCY_TIMEOUT_MS},
                    headers=_headers(),
                    timeout=LATENCY_TIMEOUT_MS / 1000 + 10,
                )
                if resp.status_code == 200:
                    delay = _normalize_delay((resp.json() or {}).get("delay"))
                    if delay > 0:
                        return delay
                else:
                    log.debug("node delay %s via %s returned HTTP %s", node_name, probe_url, resp.status_code)
            except Exception as exc:
                log.debug("node delay %s via %s failed (attempt %s/%s): %s", node_name, probe_url, attempt + 1, LATENCY_ATTEMPTS, exc)
        if attempt + 1 < LATENCY_ATTEMPTS and LATENCY_RETRY_DELAY:
            time.sleep(LATENCY_RETRY_DELAY)
    return -1


def _mihomo_outbound_delay() -> int:
    """Measure the real HTTP path through mihomo's mixed port.

    The controller's per-proxy delay API can return a false negative while the
    selected ROTATOR route is already carrying model traffic. This independent
    probe verifies the same path used by proxy-server and is used only as a
    fallback for the current-egress button.
    """
    if not MIHOMO_OUTBOUND_PROXY:
        return -1
    proxies_map = {"http": MIHOMO_OUTBOUND_PROXY, "https": MIHOMO_OUTBOUND_PROXY}
    probe_urls = list(dict.fromkeys([HEALTH_CHECK_URL, "https://cp.cloudflare.com/generate_204"]))
    for probe_url in probe_urls:
        try:
            started = time.monotonic()
            resp = requests.get(
                probe_url,
                proxies=proxies_map,
                timeout=max(12, LATENCY_TIMEOUT_MS / 1000 + 2),
                impersonate="chrome124",
            )
            if resp.status_code in (200, 204):
                elapsed = getattr(resp, "elapsed", None)
                if elapsed is not None and hasattr(elapsed, "total_seconds"):
                    return max(1, int(elapsed.total_seconds() * 1000))
                return max(1, int((time.monotonic() - started) * 1000))
        except Exception as exc:
            log.debug("mihomo outbound delay via %s failed: %s", probe_url, exc)
    return -1


def _mihomo_parallel_single_delays(
    names: List[str],
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, int]:
    """分批并发探测节点，避免 500+ 节点同时压垮 mihomo。

    之前把全部节点一次性提交给线程池时，免费节点较多的场景会让
    ``/proxies/:name/delay`` 请求互相争抢连接和控制器资源；结果不是节点
    没有，而是整批请求在同一时间窗口内超时。这里保留有限并发，并按批次
    等待本批全部结束后再提交下一批。这样返回值始终包含每个输入节点，
    一次短暂超时也不会让整个提取任务提前结束。
    """
    requested = list(dict.fromkeys(str(name).strip() for name in names if str(name).strip()))
    if not requested:
        return {}

    results: Dict[str, int] = {}
    completed = 0
    batch_size = max(1, LATENCY_BATCH_SIZE)
    # 同一时间只运行一套大批量节点探测，避免“批量测延迟”和“一键提取”
    # 同时点击后把 mihomo 的 controller 连接池翻倍。
    with _latency_probe_lock:
        for offset in range(0, len(requested), batch_size):
            batch = requested[offset:offset + batch_size]
            workers = min(LATENCY_WORKERS, len(batch))
            log.info(
                "panel node delay probe batch %s-%s/%s (workers=%s)",
                offset + 1, offset + len(batch), len(requested), workers,
            )
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="panel-delay") as executor:
                future_names = {executor.submit(_mihomo_single_delay, name): name for name in batch}
                for future in as_completed(future_names):
                    name = future_names[future]
                    try:
                        results[name] = _normalize_delay(future.result())
                    except Exception as exc:
                        log.debug("parallel node delay %s failed: %s", name, exc)
                        results[name] = -1
                    completed += 1
                    if progress_callback:
                        try:
                            progress_callback(completed, len(requested))
                        except Exception as exc:
                            log.debug("node delay progress callback failed: %s", exc)
            if offset + len(batch) < len(requested) and LATENCY_BATCH_PAUSE:
                time.sleep(LATENCY_BATCH_PAUSE)
    return results


def mihomo_test_delay(
    proxies: Optional[List[str]] = None,
    group: Optional[str] = None,
    group_first: bool = True,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, Any]:
    """同步等待 mihomo 延迟探测完成，失败返回 -1。

    ``group`` 仅保留给需要查看某个 mihomo 组整体状态的内部调用。
    节点管理的批量/选中测速统一走每个节点的 ``/proxies/:name/delay``，
    这样混合“自己的订阅 + free provider”时，每个节点都使用它自己的
    出口测试，不会把 ROTATOR 当前节点的组级结果误套到其它节点。

    ``group_first`` 为兼容旧调用保留，但不再改变节点列表测速策略。
    """
    if group:
        return _mihomo_group_delays(group)
    if not proxies:
        return {}

    requested = list(dict.fromkeys(str(name).strip() for name in proxies if str(name).strip()))
    # 无论是批量还是选中，都独立探测每个代理节点。保留无回调时的
    # 旧函数调用形态，便于现有内部调用和测试兼容。
    if progress_callback is None:
        return _mihomo_parallel_single_delays(requested)
    return _mihomo_parallel_single_delays(requested, progress_callback=progress_callback)


# -----------------------------------------------------------------------------
# config.yaml 读写（共享目录，面板可写）
# -----------------------------------------------------------------------------
def load_config() -> Optional[Dict[str, Any]]:
    if not CONFIG_FILE.exists():
        return None
    try:
        import yaml
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("load config failed: %s", e)
    return None


def save_config(cfg: Dict[str, Any]) -> bool:
    try:
        import yaml
        # 所有配置写入统一经过合法性修复，避免任一路径生成空 ROTATOR 组后
        # 让 mihomo 陷入 crash-loop。
        _ensure_rotator_group_sources(cfg)
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        return True
    except Exception as e:
        log.warning("save config failed: %s", e)
    return False


def _get_rotator_group(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for g in cfg.get("proxy-groups", []):
        if isinstance(g, dict) and g.get("name") == "ROTATOR":
            return g
    return None


def _selection_filter(names: List[str]) -> str:
    """将节点快照编码为 mihomo proxy-group 的安全正则过滤器。

    旧版本把选中的节点名直接写进 ``ROTATOR.proxies``。provider 节点在 mihomo
    重启早期尚未注册时，这种写法会触发 ``'<node>' not found`` 并导致 crash-loop。
    使用 ``use + filter`` 只引用 provider 名称，节点加载完成后再由 mihomo 按名称
    过滤，既保留“只使用选中节点”的语义，也不会在启动阶段引用不存在的代理。
    """
    values = [str(name).strip() for name in names if str(name).strip()]
    values = list(dict.fromkeys(values))
    def escape_regex(value: str) -> str:
        # 只转义 RE2/Go 正则的元字符；空格、连字符和 Unicode 不需要
        # 反斜杠，避免 mihomo 因 ``\ ``/``\-`` 报 invalid escape。
        return re.sub(r"([\\.^$*+?{}\[\]|()])", r"\\\1", value)

    return "^(?:" + "|".join(escape_regex(name) for name in values) + ")$"


def _configure_rotator_selection(cfg: Dict[str, Any], selected: Optional[List[str]]) -> bool:
    """用 provider + filter 配置 ROTATOR，避免显式引用 provider 节点。"""
    group = _get_rotator_group(cfg)
    if group is None:
        return False
    before = json.dumps(group, ensure_ascii=False, sort_keys=True)
    providers = cfg.get("proxy-providers", {}) or {}
    provider_names = [str(name) for name in providers.keys()]
    selected_names = list(dict.fromkeys(str(name).strip() for name in (selected or []) if str(name).strip()))

    if not provider_names:
        group["proxies"] = ["DIRECT"]
        group.pop("use", None)
        group.pop("filter", None)
    elif not selected_names:
        group["use"] = provider_names
        group.pop("proxies", None)
        group.pop("filter", None)
    else:
        # 只引入包含选中节点的 provider；缓存暂不可读时退回全部 provider，
        # 让远端 provider 加载完成后仍能通过 filter 找到节点。
        matched_providers: List[str] = []
        for provider_name, provider in providers.items():
            if not isinstance(provider, dict):
                continue
            names = _provider_cache_node_names(provider)
            if names.intersection(selected_names):
                matched_providers.append(str(provider_name))
        use_names = matched_providers or provider_names
        group["use"] = list(dict.fromkeys(use_names))
        group.pop("proxies", None)
        # 始终写入精确正则，避免订阅刷新后新增节点悄悄进入用户的活动快照。
        # 不存在的旧节点只会让组暂时为空，不会让 mihomo 拒绝配置。
        group["filter"] = _selection_filter(selected_names)

    after = json.dumps(group, ensure_ascii=False, sort_keys=True)
    return before != after


def _ensure_rotator_group_sources(cfg: Dict[str, Any]) -> bool:
    """保证 ROTATOR 使用稳定的 provider 引用，避免 mihomo 启动 crash-loop。

    mihomo 不接受两者同时缺失或均为空的代理组。更重要的是，不能把 provider
    内部的节点名直接写进 ``proxies``：重启时 provider 尚未完成加载会报
    ``proxy group[0]: ROTATOR: '<node>' not found``。因此旧版显式节点列表会自动
    迁移为 ``use + filter``；没有 provider 时才使用 DIRECT 哨兵。
    """
    groups = cfg.setdefault("proxy-groups", [])
    group = _get_rotator_group(cfg)
    created = False
    if group is None:
        group = {"name": "ROTATOR", "type": "select"}
        groups.append(group)
        created = True

    before = json.dumps(group, ensure_ascii=False, sort_keys=True)
    providers = cfg.get("proxy-providers", {}) or {}
    provider_names = [str(name) for name in providers.keys()]
    raw_use = group.get("use", [])
    valid_use = [str(name) for name in raw_use if str(name) in set(provider_names)] if isinstance(raw_use, list) else []
    raw_proxies = group.get("proxies", [])
    explicit_proxies = [str(name).strip() for name in raw_proxies if str(name).strip()] if isinstance(raw_proxies, list) else []

    if provider_names and explicit_proxies and explicit_proxies != ["DIRECT"]:
        # 迁移历史版本的显式节点引用；只保留节点筛选语义，不保留危险的
        # proxy name 引用。
        _configure_rotator_selection(cfg, explicit_proxies)
    elif provider_names and explicit_proxies == ["DIRECT"]:
        _configure_rotator_selection(cfg, [])
    elif provider_names:
        group["use"] = list(dict.fromkeys(valid_use or provider_names))
        group.pop("proxies", None)
    else:
        group["proxies"] = ["DIRECT"]
        group.pop("use", None)
        group.pop("filter", None)

    after = json.dumps(group, ensure_ascii=False, sort_keys=True)
    return created or before != after


def _update_provider_in_group(group: Dict[str, Any], providers: List[str]) -> None:
    """把 ROTATOR 组的 use 列表同步为当前 providers。"""
    group["use"] = providers


# -----------------------------------------------------------------------------
# 面板持久化状态（节点勾选、自定义组）
# -----------------------------------------------------------------------------
def _default_state() -> Dict[str, Any]:
    return {
        "enabled_nodes": [],        # [] = 全部启用；非空 = 只启用这些
        "groups": {},               # {组名: [节点名, ...]}，独立快照
        # 记录创建组时节点来自哪些订阅，用于订阅删除后的失效诊断。
        # 兼容旧状态：没有该字段时退化为按当前 mihomo 节点名判断。
        "group_sources": {},        # {组名: {节点名: [订阅名, ...]}}
        "active_group": None,        # 当前应用的自定义组；仅影响活动节点视图
    }


def load_state() -> Dict[str, Any]:
    try:
        if PANEL_STATE_FILE.exists():
            with open(PANEL_STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            if isinstance(st, dict):
                return st
    except Exception as e:
        log.warning("load panel state failed: %s", e)
    return _default_state()


def save_state(st: Dict[str, Any]) -> bool:
    try:
        with open(PANEL_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log.warning("save panel state failed: %s", e)
    return False


# -----------------------------------------------------------------------------
# 订阅管理
# -----------------------------------------------------------------------------
def list_subscriptions() -> List[Dict[str, Any]]:
    """列出所有订阅 provider 及其节点数。"""
    cfg = load_config() or {}
    providers = cfg.get("proxy-providers", {}) or {}
    # 从 mihomo 拿实际节点数
    live = mihomo_get("/providers/proxies") or {}
    live_prov = {}
    raw = live.get("providers", {})
    if isinstance(raw, dict):
        for name, p in raw.items():
            if isinstance(p, dict):
                live_prov[name] = p
    elif isinstance(raw, list):
        for p in raw:
            if isinstance(p, dict) and p.get("name"):
                live_prov[p["name"]] = p

    state = load_state()
    subscription_meta = state.get("subscription_meta", {}) if isinstance(state.get("subscription_meta", {}), dict) else {}
    result = []
    for name, prov in providers.items():
        if not isinstance(prov, dict):
            continue
        node_count = 0
        lp = live_prov.get(name, {})
        proxies = lp.get("proxies", [])
        if isinstance(proxies, list):
            node_count = len(proxies)
        cached_path = _resolve_provider_file(prov)
        if node_count == 0 and cached_path and cached_path.exists():
            try:
                import yaml
                cached = yaml.safe_load(cached_path.read_text(encoding="utf-8")) or {}
                if isinstance(cached, dict) and isinstance(cached.get("proxies"), list):
                    node_count = len(cached["proxies"])
                elif prov.get("format") == "text":
                    node_count = sum(1 for line in cached_path.read_text(encoding="utf-8").splitlines() if line.strip())
            except Exception:
                pass
        result.append({
            "name": name,
            "url": _subscription_source_url(name, prov, state),
            "node_count": node_count,
            "interval": prov.get("interval", 3600),
            "format": (subscription_meta.get(name, {}) or {}).get("format", "yaml"),
        })
    return result


def add_subscription(url: str) -> Dict[str, Any]:
    """添加一个订阅 provider，写入 config.yaml 并热重载。"""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return {"ok": False, "error": "无效的订阅链接（需 http/https 开头）"}
    with _lock:
        cfg = load_config() or {}
        providers = cfg.setdefault("proxy-providers", {})
        # 生成不冲突的 provider 名
        idx = 1
        while f"subscription{idx}" in providers:
            idx += 1
        name = f"subscription{idx}"
        providers[name] = {
            "type": "http",
            "url": url,
            "interval": 3600,
            "user-agent": DEFAULT_UA,
            "path": f"./providers/{name}.yaml",
            "health-check": {
                "enable": True,
                "interval": 300,
                "url": HEALTH_CHECK_URL,
            },
        }
        group = _get_rotator_group(cfg)
        if group:
            raw_use = group.get("use")
            use = [u for u in raw_use if u != name] if isinstance(raw_use, list) else []
            use.append(name)
            group["use"] = use
        _ensure_rotator_group_sources(cfg)
        state = load_state()
        state.setdefault("subscription_urls", {})[name] = url
        if not save_config(cfg) or not save_state(state):
            return {"ok": False, "error": "配置写入失败"}
    refresh_result = refresh_subscriptions([name])
    return {
        "ok": True,
        "created": True,
        "name": name,
        "refresh_ok": refresh_result.get("ok", False),
        "reload_ok": refresh_result.get("reload_ok", False),
        "total": refresh_result.get("total", 0),
        "ok_count": refresh_result.get("ok_count", 0),
        "results": refresh_result.get("results", []),
    }


def remove_subscription(name: str) -> Dict[str, Any]:
    """删除订阅 provider，写入 config.yaml 并热重载。"""
    with _lock:
        cfg = load_config() or {}
        providers = cfg.get("proxy-providers", {}) or {}
        if name not in providers:
            return {"ok": False, "error": f"订阅 '{name}' 不存在"}
        provider = providers.get(name, {}) if isinstance(providers.get(name), dict) else {}
        provider_file = _resolve_provider_file(provider)
        # 在删除订阅前记录组节点来源，兼容旧版创建的组。这样即使组创建时
        # 没有 group_sources 元数据，删除订阅后点击“应用组”也能给出明确提示。
        state = load_state()
        live_sources = _live_provider_node_sources(cfg)
        deleted_provider_nodes = _provider_cache_node_names(provider)
        if live_sources is not None:
            deleted_provider_nodes.update(
                node for node, source_names in live_sources.items() if name in source_names
            )
        groups = state.get("groups", {}) if isinstance(state.get("groups", {}), dict) else {}
        group_sources = state.setdefault("group_sources", {})
        if not isinstance(group_sources, dict):
            group_sources = {}
            state["group_sources"] = group_sources
        for group_name, group_nodes in groups.items():
            if not isinstance(group_nodes, list):
                continue
            source_map = group_sources.setdefault(group_name, {})
            if not isinstance(source_map, dict):
                source_map = {}
                group_sources[group_name] = source_map
            for node in group_nodes:
                node = str(node).strip()
                if node in deleted_provider_nodes:
                    existing = source_map.get(node, [])
                    if not isinstance(existing, list):
                        existing = []
                    source_map[node] = list(dict.fromkeys([*existing, name]))
        del providers[name]
        group = _get_rotator_group(cfg)
        group_use = group.get("use") if group else None
        if group and isinstance(group_use, list) and name in group_use:
            group["use"] = [u for u in group_use if u != name]
        if group and isinstance(group.get("proxies"), list) and deleted_provider_nodes:
            group["proxies"] = [
                node for node in group["proxies"] if str(node) not in deleted_provider_nodes
            ]
        # 删除 provider 后同步清理当前视图；provider 缓存会被下面删除，
        # 因此不能让这些旧节点继续作为 ROTATOR.proxies 写回配置。
        removed_names = {str(node).strip() for node in deleted_provider_nodes if str(node).strip()}
        if removed_names:
            excluded = state.get("excluded_nodes", [])
            if not isinstance(excluded, list):
                excluded = []
            state["excluded_nodes"] = list(dict.fromkeys([*excluded, *sorted(removed_names)]))
            enabled = state.get("enabled_nodes", [])
            if isinstance(enabled, list):
                state["enabled_nodes"] = [node for node in enabled if node not in removed_names]
            node_health = state.get("node_health", {})
            if isinstance(node_health, dict):
                for node in removed_names:
                    node_health.pop(node, None)
        _ensure_rotator_group_sources(cfg)
        # 按原配置路径清理缓存文件，避免下次重建同名订阅时读取旧节点。
        # 订阅时读取旧节点。
        if provider_file and provider_file.exists():
            try:
                provider_file.unlink()
            except OSError as exc:
                log.warning("remove subscription cache %s failed: %s", provider_file, exc)
        if isinstance(state.get("subscription_urls"), dict):
            state["subscription_urls"].pop(name, None)
        if isinstance(state.get("subscription_meta"), dict):
            state["subscription_meta"].pop(name, None)
        if not save_config(cfg) or not save_state(state):
            return {"ok": False, "error": "配置写入失败"}
    mihomo_reload()
    return {"ok": True, "name": name}


# -----------------------------------------------------------------------------
# 节点管理（勾选 = 启用哪些节点）
# -----------------------------------------------------------------------------
def _is_placeholder_node(name: str) -> bool:
    keywords = os.environ.get(
        "MIHOMO_EXCLUDE_KEYWORDS",
        "剩余流量,剩余,重置,套餐到期,到期,过期,欠费,流量提醒,距离下次,官网,客服,邮箱,联系,公告,通知,群组,订阅更新,刷新订阅,连接不上,无法连接",
    )
    return any(keyword.strip() and keyword.strip().lower() in name.lower() for keyword in keywords.split(","))


def _cached_provider_proxies(provider: Dict[str, Any]) -> List[Dict[str, Any]]:
    """读取 file/http provider 已落盘的 Clash YAML，供控制器重启时展示。"""
    path = _resolve_provider_file(provider)
    if not path or not path.exists() or not path.is_file():
        return []
    try:
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        proxies = data.get("proxies", []) if isinstance(data, dict) else []
        return [proxy for proxy in proxies if isinstance(proxy, dict)]
    except Exception as exc:
        log.debug("read cached provider nodes %s failed: %s", path, exc)
        return []


def list_nodes() -> Dict[str, Any]:
    """返回所有节点 + 当前启用状态。

    优先使用 mihomo 控制器的实时数据；控制器因配置修复正在重启、或 file
    provider 尚未出现在 API 中时，从 provider YAML 缓存回退展示，避免免费
    节点明明已经生成却在节点管理中显示 0 个。
    """
    live_response = mihomo_get("/providers/proxies")
    controller_ready = live_response is not None
    cfg = load_config() or {}
    configured_providers = cfg.get("proxy-providers", {}) or {}
    live_by_name: Dict[str, Dict[str, Any]] = {}
    raw = live_response.get("providers", {}) if isinstance(live_response, dict) else {}
    items = raw.items() if isinstance(raw, dict) else (
        [(p.get("name", ""), p) for p in raw] if isinstance(raw, list) else []
    )
    for provider_name, provider in items:
        if provider_name in configured_providers and isinstance(provider, dict):
            live_by_name[str(provider_name)] = provider

    nodes: List[Dict[str, Any]] = []
    cached_provider_names: List[str] = []
    live_node_count = 0
    cached_node_count = 0
    for prov_name, configured_provider in configured_providers.items():
        if not isinstance(configured_provider, dict):
            continue
        live_provider = live_by_name.get(str(prov_name), {})
        provider_nodes = live_provider.get("proxies", []) if isinstance(live_provider, dict) else []
        from_cache = not isinstance(provider_nodes, list) or len(provider_nodes) == 0
        if from_cache:
            provider_nodes = _cached_provider_proxies(configured_provider)
            if provider_nodes:
                cached_provider_names.append(str(prov_name))
                cached_node_count += len(provider_nodes)
        else:
            live_node_count += len(provider_nodes)

        for p in provider_nodes:
            if not isinstance(p, dict):
                continue
            node_name = str(p.get("name", ""))
            if _is_placeholder_node(node_name):
                continue
            history = p.get("history", []) if isinstance(p.get("history"), list) else []
            latest_history_delay = history[-1].get("delay") if history and isinstance(history[-1], dict) else None
            live_delay = p.get("delay")
            if not isinstance(live_delay, (int, float)) or live_delay <= 0:
                live_delay = latest_history_delay if isinstance(latest_history_delay, (int, float)) and latest_history_delay > 0 else None
            nodes.append({
                "name": p.get("name", ""),
                "provider": prov_name,
                "type": p.get("type", ""),
                "alive": False if from_cache else bool(p.get("alive")),
                "delay": int(live_delay) if live_delay is not None else None,
                "source": "cache" if from_cache else "mihomo",
            })
    st = load_state()
    node_health = st.get("node_health", {}) if isinstance(st.get("node_health", {}), dict) else {}
    provider_node_count = len(nodes)
    provider_node_names = {
        str(node.get("name"))
        for node in nodes
        if str(node.get("name") or "").strip()
    }
    excluded_nodes = set(st.get("excluded_nodes", [])) if isinstance(st.get("excluded_nodes", []), list) else set()
    nodes = [node for node in nodes if node.get("name") and node["name"] not in excluded_nodes]
    excluded_node_count = provider_node_count - len(nodes)
    active_group = st.get("active_group")
    groups = st.get("groups", {}) if isinstance(st.get("groups", {}), dict) else {}
    active_group_node_count = 0
    active_group_provider_count = 0
    active_group_visible_count = 0
    active_group_missing_nodes: List[str] = []
    active_group_excluded_nodes: List[str] = []
    if active_group and isinstance(groups.get(active_group), list):
        active_names_ordered = list(dict.fromkeys(
            str(name).strip() for name in groups[active_group] if str(name).strip()
        ))
        active_names = set(active_names_ordered)
        active_group_node_count = len(active_names_ordered)
        active_group_missing_nodes = [
            name for name in active_names_ordered if name not in provider_node_names
        ]
        active_group_excluded_nodes = [
            name for name in active_names_ordered
            if name in provider_node_names and name in excluded_nodes
        ]
        active_group_provider_count = active_group_node_count - len(active_group_missing_nodes)
        # 应用自定义组后，节点管理只展示并使用组快照中的节点。
        nodes = [node for node in nodes if node["name"] in active_names]
        active_group_visible_count = len({node["name"] for node in nodes})
    recovered = False
    for node in nodes:
        health = node_health.get(node["name"], {}) if isinstance(node_health.get(node["name"], {}), dict) else {}
        # mihomo 后台健康检查若已经恢复成功，应立即清除面板里之前累计的
        # transient failure，避免节点明明 alive 却继续显示“异常 n 次”。
        if node.get("alive") and isinstance(node.get("delay"), int) and node["delay"] > 0:
            node["status"] = "ok"
            node["fail_count"] = 0
            node["last_checked_at"] = health.get("last_checked_at")
            if health.get("status") != "ok" or health.get("fail_count"):
                health.update({"status": "ok", "fail_count": 0, "delay": node["delay"], "last_good_delay": node["delay"]})
                node_health[node["name"]] = health
                recovered = True
            continue
        if node.get("delay") is None and health.get("delay") is not None:
            node["delay"] = health.get("delay")
        node["status"] = health.get("status", "untested")
        node["fail_count"] = int(health.get("fail_count", 0) or 0)
        node["last_checked_at"] = health.get("last_checked_at")
    if recovered:
        st["node_health"] = node_health
        save_state(st)
    enabled = [name for name in st.get("enabled_nodes", []) if name not in excluded_nodes]
    nodes.sort(key=lambda n: (n["provider"], n["name"]))
    return {
        "nodes": nodes,
        "enabled": enabled,
        "groups": groups,
        "active_group": active_group,
        "provider_node_count": provider_node_count,
        "excluded_node_count": excluded_node_count,
        "active_group_node_count": active_group_node_count,
        "active_group_provider_count": active_group_provider_count,
        "active_group_visible_count": active_group_visible_count,
        "active_group_missing_count": len(active_group_missing_nodes),
        "active_group_missing_nodes": active_group_missing_nodes,
        "active_group_excluded_count": len(active_group_excluded_nodes),
        "active_group_excluded_nodes": active_group_excluded_nodes,
        "controller_ready": controller_ready,
        "live_node_count": live_node_count,
        "cached_node_count": cached_node_count,
        "cached_providers": cached_provider_names,
    }


def set_enabled_nodes(enabled: List[str], active_group: Optional[str] = None) -> Dict[str, Any]:
    """设置启用的节点列表（空列表 = 全部启用）并热重载。

    节点快照通过 ``ROTATOR.use + filter`` 表达，避免把 provider 内部节点名
    直接写到 proxy group，彻底规避 mihomo 重启期间的 ``node not found``。
    """
    enabled = list(dict.fromkeys(str(n).strip() for n in enabled if str(n).strip()))
    with _lock:
        cfg = load_config() or {}
        if not _get_rotator_group(cfg):
            return {"ok": False, "error": "ROTATOR 组不存在"}
        _configure_rotator_selection(cfg, enabled)
        if not save_config(cfg):
            return {"ok": False, "error": "配置写入失败"}
        st = load_state()
        st["enabled_nodes"] = enabled
        st["active_group"] = active_group
        save_state(st)
    mihomo_reload()
    return {"ok": True}


def clear_active_group() -> Dict[str, Any]:
    """退出当前应用组，恢复 ROTATOR 和节点管理中的全部订阅节点。"""
    result = set_enabled_nodes([], active_group=None)
    if result.get("ok"):
        result["active_group"] = None
    return result


# -----------------------------------------------------------------------------
# 自定义组管理
# -----------------------------------------------------------------------------
def list_groups() -> Dict[str, Any]:
    st = load_state()
    return {"groups": st.get("groups", {})}


def _provider_cache_node_names(provider: Dict[str, Any]) -> set[str]:
    """从 provider 缓存文件读取节点名，作为删除订阅时的离线兜底。"""
    path = _resolve_provider_file(provider)
    if not path or not path.exists() or not path.is_file():
        return set()
    try:
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        proxies = data.get("proxies", []) if isinstance(data, dict) else []
        return {
            str(proxy.get("name")).strip()
            for proxy in proxies
            if isinstance(proxy, dict) and str(proxy.get("name") or "").strip()
        }
    except Exception as exc:
        log.debug("read provider cache %s failed: %s", path, exc)
        return set()


def _live_provider_node_sources(cfg: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, List[str]]]:
    """返回当前 mihomo 中真实节点名到订阅 provider 的映射。

    ``None`` 表示控制器暂时不可访问，不能据此把所有组节点误判为失效；
    空字典则表示控制器可访问但当前没有真实订阅节点。
    """
    cfg = cfg or load_config() or {}
    configured = set((cfg.get("proxy-providers", {}) or {}).keys())
    live = mihomo_get("/providers/proxies")
    if live is None:
        return None
    raw = live.get("providers", {}) if isinstance(live, dict) else {}
    items = raw.items() if isinstance(raw, dict) else (
        [(p.get("name", ""), p) for p in raw] if isinstance(raw, list) else []
    )
    sources: Dict[str, List[str]] = {}
    for provider_name, provider in items:
        if provider_name not in configured or not isinstance(provider, dict):
            continue
        for proxy in provider.get("proxies", []):
            if not isinstance(proxy, dict):
                continue
            node_name = str(proxy.get("name") or "").strip()
            if node_name:
                sources.setdefault(node_name, []).append(str(provider_name))
    return sources


def _find_missing_group_nodes(
    nodes: List[str],
    state: Dict[str, Any],
    cfg: Dict[str, Any],
    live_sources: Optional[Dict[str, List[str]]],
) -> List[str]:
    """找出组快照中因订阅被删除/节点消失而无法应用的节点。

    优先使用创建组时保存的 provider 来源，因此同名节点来自多个订阅时，
    只要仍有一个来源订阅存在就不会被误报为失效。旧版没有来源元数据时，
    使用 mihomo 当前节点名做兼容判断。
    """
    configured = set((cfg.get("proxy-providers", {}) or {}).keys())
    all_group_sources = state.get("group_sources", {})
    source_map = all_group_sources.get(state.get("_checking_group"), {}) if isinstance(all_group_sources, dict) else {}
    if not isinstance(source_map, dict):
        source_map = {}

    missing: List[str] = []
    for node in list(dict.fromkeys(str(item).strip() for item in nodes if str(item).strip())):
        recorded_sources = source_map.get(node)
        if isinstance(recorded_sources, list) and recorded_sources:
            # 订阅被删除时 provider 名会从 config 中消失。
            if not any(str(provider) in configured for provider in recorded_sources):
                missing.append(node)
                continue
        elif live_sources is not None and node not in live_sources:
            missing.append(node)
            continue

        # 来源订阅仍存在但节点已从最新订阅内容消失，也视为不可应用。
        if live_sources is not None and node not in live_sources:
            missing.append(node)
    return missing


def _next_generated_group_name(groups: Dict[str, Any], prefix: str = "可用节点组") -> str:
    """为每次自动提取生成独立组名，避免覆盖上一次结果。"""
    existing = set(groups.keys()) if isinstance(groups, dict) else set()
    index = 1
    while f"{prefix}{index}" in existing:
        index += 1
    return f"{prefix}{index}"


def extract_healthy_group(
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """探测当前节点管理列表，并将本次探测成功的节点保存为新组。

    mihomo provider 自带的 health-check 结果也是有效探测结果。对于已经在
    节点列表显示“正常”的节点优先复用该结果，只对未检测/待确认节点调用
    ``/proxies/:name/delay`` 补测；这避免 500+ 节点重复探测造成控制器排队，
    同时保留对异常节点的独立确认。补测按批次执行，绝不会因为第一批超时
    就提前把整次任务判定为“没有可用节点”。
    """
    snapshot = list_nodes()
    snapshot_nodes = [
        node for node in snapshot.get("nodes", [])
        if isinstance(node, dict) and str(node.get("name") or "").strip()
    ]
    candidates = list(dict.fromkeys(str(node.get("name")).strip() for node in snapshot_nodes))
    if not candidates:
        return {
            "ok": False,
            "code": "no_nodes",
            "error": "当前节点列表为空，请先添加订阅或抓取免费节点",
            "measured_count": 0,
            "healthy_count": 0,
            "failed_count": 0,
        }

    snapshot_by_name = {str(node.get("name")).strip(): node for node in snapshot_nodes}
    # live mihomo provider 的 alive + delay 是 provider health-check 已确认的
    # 结果；缓存 provider 不会伪造 alive，因此不会把尚未加载的节点误认为健康。
    cached_healthy: Dict[str, int] = {}
    for name, node in snapshot_by_name.items():
        delay = _normalize_delay(node.get("delay"))
        if (node.get("status") == "ok" or node.get("alive") is True) and delay > 0 and node.get("source") == "mihomo":
            cached_healthy[name] = delay

    to_probe = [name for name in candidates if name not in cached_healthy]
    log.info(
        "extract healthy group: candidates=%s, reuse_mihomo_health=%s, probe_remaining=%s",
        len(candidates), len(cached_healthy), len(to_probe),
    )
    if progress_callback:
        progress_callback({
            "phase": "probing",
            "total": len(candidates),
            "completed": len(cached_healthy),
            "cached_healthy_count": len(cached_healthy),
            "probed_count": 0,
        })

    def report_probe_progress(completed: int, total: int) -> None:
        if progress_callback:
            progress_callback({
                "phase": "probing",
                "total": len(candidates),
                "completed": len(cached_healthy) + completed,
                "cached_healthy_count": len(cached_healthy),
                "probed_count": completed,
                "probe_total": total,
            })

    probed = (
        mihomo_test_delay(
            proxies=to_probe,
            group_first=False,
            progress_callback=report_probe_progress,
        )
        if to_probe else {}
    )
    normalized: Dict[str, int] = {}
    for name in candidates:
        direct_delay = _normalize_delay(probed.get(name))
        normalized[name] = direct_delay if direct_delay > 0 else cached_healthy.get(name, -1)
    if probed:
        _record_node_health(probed)
    # 将复用的 mihomo 健康结果也同步到组状态，但不把没参与本轮的节点
    # 写成 failed；它们本身已经由 mihomo provider health-check 确认正常。
    if cached_healthy:
        _record_node_health(cached_healthy)

    healthy = [name for name in candidates if normalized.get(name, -1) > 0]
    failed = [name for name in candidates if normalized.get(name, -1) <= 0]
    if not healthy:
        return {
            "ok": False,
            "code": "no_healthy_nodes",
            "error": (
                "本轮没有确认到可用节点；已按批次完成探测，可能是节点本身失效或 mihomo 仍在加载订阅，"
                "请等待几秒后刷新节点列表再试"
            ),
            "measured_count": len(candidates),
            "probed_count": len(to_probe),
            "cached_healthy_count": len(cached_healthy),
            "healthy_count": 0,
            "failed_count": len(failed),
            "failed_nodes": failed,
            "delays": normalized,
        }

    state = load_state()
    groups = state.get("groups", {}) if isinstance(state.get("groups", {}), dict) else {}
    group_name = _next_generated_group_name(groups)
    saved = save_group(group_name, healthy)
    if not saved.get("ok"):
        return {
            **saved,
            "measured_count": len(candidates),
            "probed_count": len(to_probe),
            "cached_healthy_count": len(cached_healthy),
            "healthy_count": len(healthy),
            "failed_count": len(failed),
        }
    return {
        "ok": True,
        "group": group_name,
        "node_count": len(healthy),
        "nodes": healthy,
        "measured_count": len(candidates),
        "probed_count": len(to_probe),
        "cached_healthy_count": len(cached_healthy),
        "healthy_count": len(healthy),
        "failed_count": len(failed),
        "failed_nodes": failed,
        "delays": normalized,
    }


def _cleanup_extract_jobs(now: Optional[float] = None) -> None:
    current = now or time.time()
    expired = [
        job_id for job_id, job in _extract_jobs.items()
        if job.get("status") in {"completed", "failed"}
        and current - float(job.get("updated_at") or job.get("created_at") or current) > EXTRACT_JOB_TTL
    ]
    for job_id in expired:
        _extract_jobs.pop(job_id, None)


def _update_extract_job(job_id: str, **changes: Any) -> None:
    with _extract_job_lock:
        job = _extract_jobs.get(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = time.time()
        total = int(job.get("total") or 0)
        completed = min(total, int(job.get("completed") or 0)) if total else 0
        job["progress"] = int(completed * 100 / total) if total else 0


def _run_extract_healthy_group_job(job_id: str) -> None:
    _update_extract_job(job_id, status="running", message="正在读取节点列表")

    def progress(update: Dict[str, Any]) -> None:
        completed = int(update.get("completed") or 0)
        total = int(update.get("total") or 0)
        _update_extract_job(
            job_id,
            status="running",
            phase=update.get("phase", "probing"),
            total=total,
            completed=completed,
            cached_healthy_count=int(update.get("cached_healthy_count") or 0),
            probed_count=int(update.get("probed_count") or 0),
            message=f"正在分批探测节点：{completed}/{total}" if total else "正在准备节点探测",
        )

    try:
        result = extract_healthy_group(progress_callback=progress)
        if result.get("ok"):
            _update_extract_job(
                job_id,
                status="completed",
                phase="completed",
                total=int(result.get("measured_count") or 0),
                completed=int(result.get("measured_count") or 0),
                progress=100,
                message="可用节点组已生成",
                result=result,
            )
        else:
            _update_extract_job(
                job_id,
                status="failed",
                phase="failed",
                total=int(result.get("measured_count") or 0),
                completed=int(result.get("measured_count") or 0),
                message=result.get("error", "未能生成可用节点组"),
                result=result,
                error=result.get("error", "未能生成可用节点组"),
            )
    except Exception as exc:
        log.exception("extract healthy group job %s failed", job_id)
        _update_extract_job(
            job_id,
            status="failed",
            phase="failed",
            message=str(exc),
            error=str(exc),
        )


def start_extract_healthy_group_job() -> Dict[str, Any]:
    """启动后台提取任务；长达数分钟的 500+ 节点探测不会占住 HTTP 请求。"""
    with _extract_job_lock:
        _cleanup_extract_jobs()
        for existing in _extract_jobs.values():
            if existing.get("status") in {"queued", "running"}:
                return {**existing, "ok": True, "reused": True}
        job_id = uuid.uuid4().hex
        now = time.time()
        job = {
            "ok": True,
            "job_id": job_id,
            "status": "queued",
            "phase": "queued",
            "total": 0,
            "completed": 0,
            "progress": 0,
            "message": "任务已排队",
            "created_at": now,
            "updated_at": now,
        }
        _extract_jobs[job_id] = job
    threading.Thread(
        target=_run_extract_healthy_group_job,
        args=(job_id,),
        name=f"extract-healthy-{job_id[:8]}",
        daemon=True,
    ).start()
    return dict(job)


def get_extract_healthy_group_job(job_id: str) -> Dict[str, Any]:
    with _extract_job_lock:
        _cleanup_extract_jobs()
        job = _extract_jobs.get(str(job_id or "").strip())
        if not job:
            return {"ok": False, "code": "job_not_found", "error": "提取任务不存在或已过期"}
        # 深拷贝，避免 FastAPI 序列化期间后台线程继续修改同一个 dict。
        return json.loads(json.dumps(job, ensure_ascii=False))


def save_group(name: str, nodes: List[str]) -> Dict[str, Any]:
    name = name.strip()
    if not name or name == "ROTATOR":
        return {"ok": False, "error": "组名不能为空且不能为 ROTATOR"}
    nodes = list(dict.fromkeys(n for n in nodes if n))
    if not nodes:
        return {"ok": False, "error": "组内至少需要 1 个节点"}
    with _lock:
        st = load_state()
        groups = st.setdefault("groups", {})
        groups[name] = nodes
        live_sources = _live_provider_node_sources()
        group_sources = st.setdefault("group_sources", {})
        if not isinstance(group_sources, dict):
            group_sources = {}
            st["group_sources"] = group_sources
        group_sources[name] = {
            node: list((live_sources or {}).get(node, []))
            for node in nodes
        }
        save_state(st)
    return {"ok": True, "group": name, "node_count": len(nodes)}


def delete_group(name: str) -> Dict[str, Any]:
    """删除组快照；如果删除的是当前应用组，同时恢复全部订阅节点。"""
    was_active = False
    with _lock:
        st = load_state()
        groups = st.get("groups", {})
        if name in groups:
            del groups[name]
            group_sources = st.get("group_sources")
            if isinstance(group_sources, dict):
                group_sources.pop(name, None)
            was_active = st.get("active_group") == name
            if was_active:
                st["active_group"] = None
            save_state(st)
    if was_active:
        result = set_enabled_nodes([], active_group=None)
        if not result.get("ok"):
            return result
    return {"ok": True, "deleted": name, "restored_all_nodes": was_active}


def apply_group(name: str) -> Dict[str, Any]:
    """应用独立组快照；组中有已删除订阅节点时先阻止应用并返回诊断。"""
    st = load_state()
    groups = st.get("groups", {}) if isinstance(st.get("groups", {}), dict) else {}
    nodes = list(dict.fromkeys(groups.get(name) or []))
    if not nodes:
        return {"ok": False, "error": f"组 '{name}' 不存在或为空"}

    # 先更新仍存在的订阅，尽可能让组快照中的节点恢复。
    try:
        refresh_subscriptions()
    except Exception as exc:
        log.warning("refresh before applying group '%s' failed: %s", name, exc)

    cfg = load_config() or {}
    live_sources = _live_provider_node_sources(cfg)
    # 让兼容 helper 能按当前组读取来源元数据，避免改变其公共参数。
    check_state = dict(st)
    check_state["_checking_group"] = name
    missing = _find_missing_group_nodes(nodes, check_state, cfg, live_sources)
    if missing:
        configured = set((cfg.get("proxy-providers", {}) or {}).keys())
        group_sources = st.get("group_sources", {})
        source_map = group_sources.get(name, {}) if isinstance(group_sources, dict) else {}
        missing_subscriptions = sorted({
            str(provider)
            for node in missing
            for provider in (source_map.get(node, []) if isinstance(source_map, dict) else [])
            if str(provider) not in configured
        })
        return {
            "ok": False,
            "code": "group_nodes_missing",
            "group": name,
            "node_count": len(nodes),
            "missing_nodes": missing,
            "missing_count": len(missing),
            "available_count": len(nodes) - len(missing),
            "missing_subscriptions": missing_subscriptions,
            "error": (
                f"组‘{name}’中有 {len(missing)} 个节点所属订阅已删除或节点已失效，请重新导入订阅后再应用。"
            ),
        }

    excluded = st.get("excluded_nodes", []) if isinstance(st.get("excluded_nodes", []), list) else []
    st["excluded_nodes"] = [node for node in excluded if node not in set(nodes)]
    save_state(st)

    result = set_enabled_nodes(nodes, active_group=name)
    if result.get("ok"):
        result.update({"group": name, "node_count": len(nodes), "nodes": nodes})
    return result


# -----------------------------------------------------------------------------
# 延迟测试（面板用）
# -----------------------------------------------------------------------------
NODE_FAILURE_THRESHOLD = max(1, int(os.environ.get("NODE_FAILURE_THRESHOLD", "3")))


def _node_health_state() -> Dict[str, Any]:
    state = load_state()
    health = state.get("node_health", {})
    return health if isinstance(health, dict) else {}


def _record_node_health(delays: Dict[str, Any]) -> None:
    """把一次批量探测结果归一化保存，供清理任务判断连续失败。"""
    if not delays:
        return
    with _lock:
        state = load_state()
        health = state.setdefault("node_health", {})
        checked_at = int(time.time())
        for name, raw_delay in delays.items():
            try:
                delay = int(raw_delay)
            except (TypeError, ValueError):
                delay = -1
            item = health.setdefault(str(name), {"fail_count": 0})
            if delay == -1:
                item["fail_count"] = int(item.get("fail_count", 0)) + 1
                item["last_probe_delay"] = -1
                # 单次超时很可能只是 mihomo 正在探测或节点短时抖动，不立即判死。
                # 连续达到阈值后才写入 failed / delay=-1，避免“一测全红，稍后又恢复”。
                if item["fail_count"] >= NODE_FAILURE_THRESHOLD:
                    item["status"] = "failed"
                    item["delay"] = -1
                else:
                    item["status"] = "suspect"
                    item["delay"] = item.get("last_good_delay")
            else:
                item["fail_count"] = 0
                item["status"] = "ok"
                item["delay"] = delay
                item["last_good_delay"] = delay
                item["last_probe_delay"] = delay
            item["last_checked_at"] = checked_at
        save_state(state)


def _cleanup_delay_jobs(now: Optional[float] = None) -> None:
    current = now or time.time()
    expired = [
        job_id for job_id, job in _delay_jobs.items()
        if job.get("status") in {"completed", "failed"}
        and current - float(job.get("updated_at") or job.get("created_at") or current) > DELAY_JOB_TTL
    ]
    for job_id in expired:
        _delay_jobs.pop(job_id, None)


def _update_delay_job(job_id: str, **changes: Any) -> None:
    with _delay_job_lock:
        job = _delay_jobs.get(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = time.time()
        total = int(job.get("total") or 0)
        completed = min(total, int(job.get("completed") or 0)) if total else 0
        job["progress"] = int(completed * 100 / total) if total else 0


def _run_node_delay_job(job_id: str, names: List[str]) -> None:
    _update_delay_job(job_id, status="running", message="正在分批探测节点")

    def progress(completed: int, total: int) -> None:
        _update_delay_job(
            job_id,
            status="running",
            completed=completed,
            total=total,
            message=f"正在分批探测节点：{completed}/{total}",
        )

    try:
        delays = mihomo_test_delay(proxies=names, group_first=False, progress_callback=progress)
        _record_node_health(delays)
        _update_delay_job(
            job_id,
            status="completed",
            phase="completed",
            completed=len(names),
            total=len(names),
            progress=100,
            message="延迟探测完成",
            result=delays,
        )
    except Exception as exc:
        log.exception("node delay job %s failed", job_id)
        _update_delay_job(job_id, status="failed", phase="failed", message=str(exc), error=str(exc))


def start_node_delay_job(node_names: Optional[List[str]] = None) -> Dict[str, Any]:
    names = list(dict.fromkeys(str(name).strip() for name in (node_names or []) if str(name).strip()))
    if not names:
        snapshot = list_nodes()
        names = list(dict.fromkeys(
            str(node.get("name") or "").strip()
            for node in snapshot.get("nodes", [])
            if isinstance(node, dict) and str(node.get("name") or "").strip()
        ))
    if not names:
        return {"ok": False, "code": "no_nodes", "error": "当前节点列表为空"}
    with _delay_job_lock:
        _cleanup_delay_jobs()
        for existing in _delay_jobs.values():
            if existing.get("status") in {"queued", "running"}:
                return {**existing, "ok": True, "reused": True}
        job_id = uuid.uuid4().hex
        now = time.time()
        job = {
            "ok": True,
            "job_id": job_id,
            "status": "queued",
            "phase": "queued",
            "total": len(names),
            "completed": 0,
            "progress": 0,
            "message": "任务已排队",
            "created_at": now,
            "updated_at": now,
        }
        _delay_jobs[job_id] = job
    threading.Thread(
        target=_run_node_delay_job,
        args=(job_id, names),
        name=f"node-delay-{job_id[:8]}",
        daemon=True,
    ).start()
    return dict(job)


def get_node_delay_job(job_id: str) -> Dict[str, Any]:
    with _delay_job_lock:
        _cleanup_delay_jobs()
        job = _delay_jobs.get(str(job_id or "").strip())
        if not job:
            return {"ok": False, "code": "job_not_found", "error": "测速任务不存在或已过期"}
        return json.loads(json.dumps(job, ensure_ascii=False))


def test_nodes_delay(node_names: Optional[List[str]] = None, probe_mode: str = "batch") -> Dict[str, Any]:
    """测试指定节点或全部节点的延迟并保存状态。

    ``selected`` 只探测选中节点；``batch`` 如果未传节点名，则从当前节点
    列表生成完整快照后逐节点探测。这里不再回退到 ROTATOR 组级测速，避免
    混合订阅时把一个当前出口的结果套给全部节点。
    """
    mode = (probe_mode or "batch").strip().lower()
    names = list(node_names or [])
    if not names:
        snapshot = list_nodes()
        names = [
            str(node.get("name") or "").strip()
            for node in snapshot.get("nodes", [])
            if isinstance(node, dict) and str(node.get("name") or "").strip()
        ]
    delays = mihomo_test_delay(proxies=names, group_first=mode != "selected") if names else {}
    _record_node_health(delays)
    return delays


def test_current_node_delay() -> Dict[str, Any]:
    """测试模型请求当前实际使用出口的延迟。

    统一调度启用后，模型出口可能是自定义代理池，也可能是 mihomo
    ROTATOR。这里必须与 server.get_next_outbound_proxy() 使用相同的优先级，
    不能再固定查询 ROTATOR，否则仅配置 SOCKS5/HTTP 代理时会误报无节点。
    """
    proxies = _read_proxy_file()
    route = proxy_pool.routing_snapshot()
    hold_mihomo = route.get("mode") == "mihomo" and route.get("hold_mihomo") is True

    # 非 ROTATOR 保持阶段与模型请求一致：优先选择当前健康代理。
    current_proxy = None if hold_mihomo else proxy_pool.select_active_proxy(proxies)
    if current_proxy:
        checked = check_proxies([current_proxy])
        result = (checked.get("results") or [{}])[0]
        ok = bool(result.get("ok"))
        return {
            "ok": ok,
            "egress": "proxy",
            "node": current_proxy,
            "delay": result.get("latency"),
            **({} if ok else {"error": "当前代理池节点测速失败或超时，已进入冷却"}),
        }

    # 手动轮换正在保持 mihomo，或代理池没有可用节点时，测试 ROTATOR 当前节点。
    group_info = mihomo_get("/proxies/ROTATOR") or {}
    current = str(group_info.get("now") or "").strip()
    if current and current != "ROTATOR":
        delays = mihomo_test_delay(proxies=[current], group_first=False)
        delay = delays.get(current, -1)
        if delay != -1:
            _record_node_health({current: delay})
            return {"ok": True, "egress": "mihomo", "node": current, "delay": delay}

        # 单节点 controller API 偶尔会与 ROTATOR 切换/health-check 竞争而误报。
        # 再沿模型请求实际使用的 mihomo:7890 出站探测一次；成功即说明当前
        # 真实出口可用，不应向用户显示“延迟测试失败”。
        route_delay = _mihomo_outbound_delay()
        after_info = mihomo_get("/proxies/ROTATOR") or {}
        after_current = str(after_info.get("now") or "").strip()
        if route_delay > 0:
            measured_node = after_current if after_current and after_current != "ROTATOR" else current
            if measured_node == current:
                _record_node_health({current: route_delay})
            return {
                "ok": True,
                "egress": "mihomo",
                "node": measured_node,
                "delay": route_delay,
                "probe": "outbound-fallback",
                "note": "单节点测速接口未返回延迟，已通过模型请求实际出站链路确认可用",
            }

        _record_node_health({current: -1})
        return {
            "ok": False,
            "egress": "mihomo",
            "node": current,
            "delay": -1,
            "error": "当前 mihomo ROTATOR 节点及实际出站链路均测速失败或超时",
        }

    # egress_state 可能残留一次旧的 mihomo hold；若实际没有订阅节点但代理池
    # 仍有健康代理，则立即恢复到代理池并完成本次测速。
    available = proxy_pool.eligible_proxies(proxies)
    if available:
        current_proxy = available[0]
        proxy_pool.set_active_proxy(current_proxy, reason="current-delay fallback without ROTATOR node")
        checked = check_proxies([current_proxy])
        result = (checked.get("results") or [{}])[0]
        ok = bool(result.get("ok"))
        return {
            "ok": ok,
            "egress": "proxy",
            "node": current_proxy,
            "delay": result.get("latency"),
            **({} if ok else {"error": "当前代理池节点测速失败或超时，已进入冷却"}),
        }

    if proxies:
        error = "代理池暂无健康节点（可能正在冷却），且当前 ROTATOR 组没有可用节点"
    else:
        error = "当前没有可测速的代理池节点或 mihomo ROTATOR 节点"
    return {"ok": False, "egress": "none", "error": error}


def _resolve_provider_file(provider: Dict[str, Any]) -> Optional[Path]:
    path = provider.get("path") if isinstance(provider, dict) else None
    if not path:
        return None
    candidate = Path(str(path))
    if not candidate.is_absolute():
        candidate = MIHOMO_CONFIG_DIR / candidate
    try:
        return candidate.resolve()
    except OSError:
        return candidate


def _remove_nodes_from_provider_file(path: Path, names: set[str]) -> List[str]:
    """从本地 provider 缓存中删除节点；远端 provider 无本地文件时由组过滤兜底。"""
    if not path.exists() or not path.is_file():
        return []
    try:
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        proxies = data.get("proxies") if isinstance(data, dict) else None
        if not isinstance(proxies, list):
            return []
        removed = [str(proxy.get("name")) for proxy in proxies if isinstance(proxy, dict) and proxy.get("name") in names]
        if not removed:
            return []
        data["proxies"] = [proxy for proxy in proxies if not (isinstance(proxy, dict) and proxy.get("name") in names)]
        path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False), encoding="utf-8")
        return removed
    except Exception as exc:
        log.warning("remove nodes from provider %s failed: %s", path, exc)
        return []


def cleanup_failed_nodes(failure_threshold: int = NODE_FAILURE_THRESHOLD) -> Dict[str, Any]:
    """清理失败实例并从活动 ROTATOR 池中移除。

    条件：最近状态为 failed、最近延迟为 -1，或连续失败次数达到阈值。
    对本地 provider 文件会同步删除节点；远端 provider 若无法持久化，则至少
    从 ROTATOR 的显式节点列表和面板选择状态中移除，避免继续被选中。
    """
    threshold = max(1, int(failure_threshold or NODE_FAILURE_THRESHOLD))
    snapshot = list_nodes()
    health = _node_health_state()
    candidates: set[str] = set()
    reasons: Dict[str, List[str]] = {}
    for node in snapshot.get("nodes", []):
        name = str(node.get("name") or "")
        state = health.get(name, {}) if isinstance(health.get(name, {}), dict) else {}
        delay = node.get("delay")
        if delay is None:
            delay = state.get("delay")
        status = node.get("status") or state.get("status")
        fail_count = int(node.get("fail_count", state.get("fail_count", 0)) or 0)
        node_reasons = []
        if status == "failed":
            node_reasons.append("failed")
        if delay == -1:
            node_reasons.append("delay=-1")
        if fail_count >= threshold:
            node_reasons.append(f"fail_count>={threshold}")
        if name and node_reasons:
            candidates.add(name)
            reasons[name] = node_reasons

    if not candidates:
        return {"ok": True, "removed": [], "count": 0, "threshold": threshold, "reasons": {}}

    with _lock:
        cfg = load_config() or {}
        group = _get_rotator_group(cfg)
        if group:
            # 不修改 provider 文件和自定义组快照；只从当前活动集合中移除。
            remaining = [
                str(node.get("name") or "").strip()
                for node in snapshot.get("nodes", [])
                if str(node.get("name") or "").strip() not in candidates
            ]
            _configure_rotator_selection(cfg, remaining)
        if not save_config(cfg):
            return {"ok": False, "error": "配置写入失败", "removed": []}

        state = load_state()
        excluded = state.get("excluded_nodes", [])
        if not isinstance(excluded, list):
            excluded = []
        state["excluded_nodes"] = list(dict.fromkeys([*excluded, *sorted(candidates)]))
        enabled = state.get("enabled_nodes", [])
        if isinstance(enabled, list):
            state["enabled_nodes"] = [name for name in enabled if name not in candidates]
        # groups 是独立快照，节点清理不能修改它；只有“删除组”才删除快照。
        node_health = state.get("node_health", {})
        if isinstance(node_health, dict):
            for name in candidates:
                node_health.pop(name, None)
        save_state(state)
    mihomo_reload()
    return {
        "ok": True,
        "removed": sorted(candidates),
        "count": len(candidates),
        "persisted_count": 0,
        "threshold": threshold,
        "reasons": reasons,
    }



def remove_selected_nodes(node_names: List[str]) -> Dict[str, Any]:
    """从节点管理/当前活动集合移除节点，但保留 provider 数据与自定义组快照。"""
    candidates = {str(name).strip() for name in (node_names or []) if str(name).strip()}
    if not candidates:
        return {"ok": False, "error": "请先选择至少一个节点"}

    visible_nodes = list_nodes().get("nodes", [])
    visible_names = {str(node.get("name")) for node in visible_nodes}
    candidates &= visible_names
    if not candidates:
        return {"ok": False, "error": "所选节点已不存在或不在当前活动集合"}
    remaining = [str(node.get("name")) for node in visible_nodes if str(node.get("name")) not in candidates]

    with _lock:
        cfg = load_config() or {}
        group = _get_rotator_group(cfg)
        if group:
            # 删除后 ROTATOR 只使用剩余节点；绝不删除 provider 文件。
            _configure_rotator_selection(cfg, remaining)
        if not save_config(cfg):
            return {"ok": False, "error": "配置写入失败"}

        state = load_state()
        excluded = state.get("excluded_nodes", []) if isinstance(state.get("excluded_nodes", []), list) else []
        state["excluded_nodes"] = list(dict.fromkeys([*excluded, *sorted(candidates)]))
        state["enabled_nodes"] = remaining
        # 自定义组是不可变快照，此处故意不修改 state["groups"]。
        health = state.get("node_health", {})
        if isinstance(health, dict):
            for node in candidates:
                health.pop(node, None)
        save_state(state)
    reload_ok = mihomo_reload()
    return {
        "ok": True,
        "removed": sorted(candidates),
        "count": len(candidates),
        "remaining_count": len(remaining),
        "persisted_count": 0,
        "reload_ok": reload_ok,
    }


# -----------------------------------------------------------------------------
# 代理池管理（手动维护出站代理列表 proxies.txt）
# 与 server.py 的 PROXY_FILE 共享同一文件；health check 测延迟/连通性
# -----------------------------------------------------------------------------
PROXY_LIST_FILE = Path(os.environ.get("PROXY_LIST_FILE", "/app/data/proxies.txt"))
# 代理状态持久化（延迟/失败次数/冷却至）
PROXY_STATE_FILE = Path(os.environ.get("PROXY_STATE_FILE", "/app/data/proxy_state.json"))

# 冷却时间（秒）：失败次数越多，冷却越长
PROXY_COOLDOWN_BASE = int(os.environ.get("PROXY_COOLDOWN_BASE", "60"))
PROXY_COOLDOWN_FACTOR = int(os.environ.get("PROXY_COOLDOWN_FACTOR", "30"))


def _read_proxy_file() -> List[str]:
    """读取 proxies.txt，返回去重后的代理列表。"""
    if not PROXY_LIST_FILE.exists():
        return []
    try:
        with open(PROXY_LIST_FILE, "r", encoding="utf-8") as f:
            lines = [normalize_proxy_url(ln) for ln in f if ln.strip() and not ln.startswith("#")]
        return list(dict.fromkeys(lines))
    except Exception as e:
        log.warning("read proxies.txt failed: %s", e)
        return []


def _write_proxy_file(proxies: List[str]) -> bool:
    try:
        PROXY_LIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(PROXY_LIST_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(proxies) + ("\n" if proxies else ""))
        return True
    except Exception as e:
        log.warning("write proxies.txt failed: %s", e)
    return False


def _valid_proxy(addr: str) -> bool:
    addr = addr.strip()
    if not addr.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return False
    # 简单校验 host:port 存在
    rest = addr.split("://", 1)[1]
    if "@" in rest:
        rest = rest.split("@", 1)[1]
    return ":" in rest


def _load_proxy_state() -> Dict[str, Any]:
    return proxy_pool.read_proxy_state()


def _save_proxy_state(st: Dict[str, Any]) -> bool:
    return proxy_pool.write_proxy_state(st)


def list_proxies() -> Dict[str, Any]:
    """返回代理列表 + 每个代理的状态（延迟/失败次数/冷却至/状态）。"""
    proxies = _read_proxy_file()
    state = _load_proxy_state()
    now = time.time()
    items = []
    for addr in proxies:
        s = state.get(addr, {})
        cooldown_until = s.get("cooldown_until", 0)
        # 冷却中判定
        if cooldown_until and now < cooldown_until:
            status = "cooldown"
            remain = int(cooldown_until - now)
        else:
            status = s.get("status", "untested")  # untested / ok / fail
            remain = 0
        items.append({
            "addr": addr,
            "status": status,
            "latency": s.get("latency"),           # ms 或 None
            "fail_count": s.get("fail_count", 0),
            "cooldown_until": s.get("cooldown_until", 0),
            "cooldown_remain": remain,
        })
    return {"proxies": items, "total": len(items)}


def add_proxies(text: str) -> Dict[str, Any]:
    """批量添加代理（每行一个，自动去重，跳过无效格式）。"""
    proxies = _read_proxy_file()
    existing = set(proxies)
    added, skipped = [], []
    for line in text.splitlines():
        addr = normalize_proxy_url(line)
        if not addr or addr.startswith("#"):
            continue
        if not _valid_proxy(addr):
            skipped.append(addr)
            continue
        if addr in existing:
            continue
        proxies.append(addr)
        existing.add(addr)
        added.append(addr)
    ok = _write_proxy_file(proxies) if added else True
    return {"ok": ok, "added": added, "skipped": skipped, "total": len(proxies)}


def remove_proxy(addr: str) -> Dict[str, Any]:
    proxies = _read_proxy_file()
    if addr not in proxies:
        return {"ok": False, "error": "代理不存在"}
    proxies.remove(addr)
    ok = _write_proxy_file(proxies)
    # 清理状态
    st = _load_proxy_state()
    st.pop(addr, None)
    _save_proxy_state(st)
    proxy_pool.clear_active_proxy_if_matches(addr)
    return {"ok": ok, "addr": addr}


def _test_one_proxy(addr: str) -> Dict[str, Any]:
    """测试单个代理连通性，返回延迟(ms) 或 None（失败）。"""
    runtime_addr = normalize_proxy_url(addr)
    proxies_map = {"http": runtime_addr, "https": runtime_addr}
    try:
        resp = requests.get(
            "https://www.gstatic.com/generate_204",
            proxies=proxies_map,
            timeout=8,
            impersonate="chrome124",
        )
        if resp.status_code in (200, 204):
            # 粗略延迟：以请求耗时计
            return {"ok": True, "latency": int(resp.elapsed.total_seconds() * 1000)}
    except Exception as e:
        log.debug("proxy test %s failed: %s", addr, e)
    return {"ok": False, "latency": None}


def check_proxies(addrs: Optional[List[str]] = None) -> Dict[str, Any]:
    """健康检查：测试指定代理或全部。更新状态并持久化。"""
    proxies = _read_proxy_file()
    if addrs:
        requested = {normalize_proxy_url(a) for a in addrs}
        targets = [a for a in proxies if a in requested]
    else:
        targets = proxies
    state = _load_proxy_state()
    now = time.time()
    results = []
    for addr in targets:
        res = _test_one_proxy(addr)
        s = state.setdefault(addr, {"fail_count": 0})
        if res["ok"]:
            s["status"] = "ok"
            s["latency"] = res["latency"]
            s["fail_count"] = 0
            s.pop("cooldown_until", None)
            results.append({"addr": addr, "ok": True, "latency": res["latency"]})
        else:
            s["fail_count"] = s.get("fail_count", 0) + 1
            s["status"] = "fail"
            s["latency"] = None
            # 熔断：失败次数越多冷却越长
            cooldown = PROXY_COOLDOWN_BASE + s["fail_count"] * PROXY_COOLDOWN_FACTOR
            s["cooldown_until"] = now + cooldown
            results.append({"addr": addr, "ok": False, "latency": None, "fail_count": s["fail_count"], "cooldown": cooldown})
    _save_proxy_state(state)
    ok_count = sum(1 for r in results if r["ok"])
    return {"ok": True, "total": len(results), "ok_count": ok_count, "results": results}

# -----------------------------------------------------------------------------
# 免费节点模块（无订阅用户）：抓取 ConfigForge-V2Ray 免费节点
# 通过 free_nodes.py 生成 providers/free.yaml，然后加入 ROTATOR 组
# -----------------------------------------------------------------------------
FREE_NODES_SCRIPT = Path(__file__).parent / "free_nodes.py"
FREE_PROVIDER_NAME = "free"
FREE_NODES_MAX = int(os.environ.get("FREE_NODES_MAX", "50"))


def get_free_node_region_options() -> List[Dict[str, str]]:
    from free_nodes import get_region_options
    return get_region_options()


def _free_nodes_command() -> List[str]:
    """Return the helper command for source and frozen portable builds."""
    if getattr(sys, "frozen", False):
        helper = Path(sys.executable).with_name("opencode-free-nodes.exe")
        if helper.exists():
            return [str(helper)]
        raise RuntimeError(f"便携版缺少免费节点抓取组件：{helper}")
    return [sys.executable, str(FREE_NODES_SCRIPT)]


def fetch_free_nodes(
    max_nodes: int = FREE_NODES_MAX,
    region_mode: str = "auto",
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """按地域筛选抓取免费节点，生成 providers/free.yaml 并热重载。"""
    import subprocess
    try:
        out_dir = str(MIHOMO_CONFIG_DIR)
        result = subprocess.run(
            [*_free_nodes_command(), "--max", str(max_nodes), "--region-mode", region_mode,
             *(["--region", region] if region else []), "--json", "--dir", out_dir,
             "--out", f"providers/{FREE_PROVIDER_NAME}.yaml"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            return {"ok": False, "error": result.stderr.strip() or "抓取失败"}
        try:
            fetch_meta = json.loads(result.stdout.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            fetch_meta = {"region_mode": region_mode, "region": region}
    except Exception as e:
        return {"ok": False, "error": f"抓取异常: {e}"}

    # 读取生成的节点数
    yaml_path = MIHOMO_CONFIG_DIR / "providers" / f"{FREE_PROVIDER_NAME}.yaml"
    node_count = 0
    try:
        import yaml
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        node_count = len(data.get("proxies", []))
    except Exception:
        pass
    if node_count == 0:
        return {"ok": False, "error": "未获取到有效节点"}

    # 加入 mihomo config：free provider 作为 file 类型
    with _lock:
        cfg = load_config() or {}
        providers = cfg.setdefault("proxy-providers", {})
        providers[FREE_PROVIDER_NAME] = {
            "type": "file",
            "path": f"./providers/{FREE_PROVIDER_NAME}.yaml",
            "health-check": {
                "enable": True,
                "interval": 300,
                "url": HEALTH_CHECK_URL,
                "lazy": False,
            },
        }
        group = _get_rotator_group(cfg)
        if group:
            explicit = group.get("proxies") if isinstance(group.get("proxies"), list) else []
            # provider 驱动模式加入 free；用户已应用节点快照时保持原组不变。
            if not explicit or explicit == ["DIRECT"]:
                raw_use = group.get("use")
                use = list(raw_use) if isinstance(raw_use, list) else []
                if FREE_PROVIDER_NAME not in use:
                    use.append(FREE_PROVIDER_NAME)
                group["use"] = use
                if explicit == ["DIRECT"]:
                    group.pop("proxies", None)
        _ensure_rotator_group_sources(cfg)
        if not save_config(cfg):
            return {"ok": False, "error": "配置写入失败"}
    with _lock:
        state = load_state()
        state["free_nodes"] = {
            "region_mode": region_mode,
            "region": fetch_meta.get("region", region),
            "region_summary": fetch_meta.get("region_summary", {}),
            "updated_at": int(time.time()),
        }
        excluded = state.get("excluded_nodes", [])
        if isinstance(excluded, list):
            free_names = _provider_cache_node_names(providers[FREE_PROVIDER_NAME])
            state["excluded_nodes"] = [name for name in excluded if name not in free_names]
        save_state(state)
    reload_ok = mihomo_reload()
    healthcheck_ok = mihomo_provider_healthcheck(FREE_PROVIDER_NAME) if reload_ok else False
    return {"ok": True, "reload_ok": reload_ok, "healthcheck_ok": healthcheck_ok, "node_count": node_count, **fetch_meta}


def get_free_nodes_status() -> Dict[str, Any]:
    """免费节点模块状态：是否已启用 + 节点数。"""
    cfg = load_config() or {}
    providers = cfg.get("proxy-providers", {}) or {}
    enabled = FREE_PROVIDER_NAME in providers
    node_count = 0
    if enabled:
        live = mihomo_get("/providers/proxies") or {}
        raw = live.get("providers", {})
        prov = raw.get(FREE_PROVIDER_NAME, {}) if isinstance(raw, dict) else {}
        if isinstance(prov.get("proxies"), list):
            node_count = len(prov["proxies"])
        if node_count == 0 and isinstance(providers.get(FREE_PROVIDER_NAME), dict):
            node_count = len(_cached_provider_proxies(providers[FREE_PROVIDER_NAME]))
    state = load_state()
    free_state = state.get("free_nodes", {}) if isinstance(state.get("free_nodes", {}), dict) else {}
    return {
        "enabled": enabled,
        "node_count": node_count,
        "region_mode": free_state.get("region_mode", "auto"),
        "region": free_state.get("region"),
        "region_summary": free_state.get("region_summary", {}),
        "region_options": get_free_node_region_options(),
    }
