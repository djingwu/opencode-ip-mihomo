import ctypes
import json
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from proxy_utils import normalize_proxy_url
import proxy_pool

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# 健康检查端点：用中性的连通性探测地址（gstatic generate_204 返回 204）。
# 不要用 opencode.ai 首页——它对机房/代理 IP 有风控，会误触发轮换。
# 实际 API 调用链路的限流处理在 server.py 的 429 逻辑里完成。
# 探测间隔 60s 已足够：节点故障兜底还有 mihomo health-check(300s) 和 server.py 429 即时换节点
CHECK_ENDPOINT = os.environ.get("WARP_CHECK_ENDPOINT", "https://www.gstatic.com/generate_204")
CHECK_INTERVAL = int(os.environ.get("WARP_CHECK_INTERVAL", "60"))
PERIODIC_ROTATION_INTERVAL = int(os.environ.get("WARP_ROTATION_INTERVAL", "300"))

INITIAL_RETRY_DELAY = int(os.environ.get("WARP_RETRY_DELAY", "3"))
MAX_RETRIES = int(os.environ.get("WARP_MAX_RETRIES", "5"))
AUTO_RECYCLE_THRESHOLD = int(os.environ.get("AUTO_RECYCLE_THRESHOLD", "50"))
CUSTOM_OUTBOUND_PROXY = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()

# -----------------------------------------------------------------------------
# mihomo (Clash Meta) Proxy Engine — 替代 WARP 作为主出口
# 通过 external-controller API 切换 ROTATOR 组内的机场节点
# -----------------------------------------------------------------------------
MIHOMO_API_URL = os.environ.get("MIHOMO_API_URL", "").strip().rstrip("/")
MIHOMO_GROUP = os.environ.get("MIHOMO_GROUP", "ROTATOR")
MIHOMO_SECRET = os.environ.get("MIHOMO_SECRET", "").strip()
# 走 mihomo 出口做验证时用的代理地址（默认容器网络内 mihomo 混合端口）
MIHOMO_OUTBOUND_PROXY = os.environ.get("MIHOMO_OUTBOUND_PROXY", "http://mihomo:7890").strip()
MIHOMO_SWITCH_ATTEMPTS = int(os.environ.get("MIHOMO_SWITCH_ATTEMPTS", "3"))
MIHOMO_VERIFY_SLEEP = float(os.environ.get("MIHOMO_VERIFY_SLEEP", "2"))
# 信息占位节点黑名单：机场订阅里"剩余流量/套餐到期"这类节点能连通但不是真实出口，
# 健康检查无法识别，必须按名称关键词排除。逗号分隔。
MIHOMO_EXCLUDE_KEYWORDS = os.environ.get(
    "MIHOMO_EXCLUDE_KEYWORDS",
    "剩余流量,剩余,重置,套餐到期,到期,过期,欠费,流量提醒,距离下次,官网,客服,邮箱,联系,公告,通知,群组,订阅更新,刷新订阅,连接不上,无法连接",
).split(",")
# 节点延迟上限（毫秒）：mihomo health-check 测得的 VPS→节点往返延迟超过该值的节点
# 会被跳过（0 = 不限制）。建议 200-300，体感敏感设 200。
MIHOMO_MAX_LATENCY = int(os.environ.get("MIHOMO_MAX_LATENCY", "300"))

# Proxy Pool Configuration
PROXY_LIST_FILE = os.environ.get("PROXY_LIST_FILE", "/app/data/proxies.txt")
PROXY_LIST_ENV = os.environ.get("PROXY_LIST", "").strip()
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()

def load_proxy_list() -> None:
    """Load proxy list from file and environment variable."""
    global _proxy_pool, _proxy_index
    proxies = []
    
    # Load from file
    proxy_file = Path(PROXY_LIST_FILE)
    if proxy_file.exists():
        try:
            with open(proxy_file, "r", encoding="utf-8") as f:
                lines = [normalize_proxy_url(line) for line in f if line.strip() and not line.startswith("#")]
                proxies.extend(lines)
        except Exception as e:
            log.error(f"Error reading proxy list file {PROXY_LIST_FILE}: {e}")
    
    # Load from environment variable
    if PROXY_LIST_ENV:
        proxies.extend([normalize_proxy_url(p) for p in PROXY_LIST_ENV.split(",") if p.strip()])
    
    # Deduplicate while preserving order
    _proxy_pool = list(dict.fromkeys(proxies))
    _proxy_index = 0
    
    if _proxy_pool:
        log.info(f"Loaded {len(_proxy_pool)} proxies into rotation pool.")
    else:
        log.info("No proxies configured. WARP rotation will be the only IP rotation method.")

def get_next_proxy() -> Optional[Dict[str, str]]:
    """Get the next proxy from the pool in round-robin fashion."""
    global _proxy_index
    with _proxy_lock:
        if not _proxy_pool:
            return None
        proxy_url = _proxy_pool[_proxy_index % len(_proxy_pool)]
        _proxy_index += 1
        return {"http": proxy_url, "https": proxy_url}

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

LOG_FORMAT = os.environ.get("LOG_FORMAT", "text").lower()
if LOG_FORMAT == "json":
    _handler = logging.StreamHandler()
    _handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[_handler])
else:
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
log = logging.getLogger("rotator")

rotation_lock = threading.Lock()
active_flows_count = 0
flow_lock = threading.Lock()
_current_ip: Optional[str] = None
rotation_count = 0
FLOW_LEASE_DB_PATH = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def has_active_flow_leases() -> bool:
    """Read proxy-owned stream leases from the shared metrics database."""
    if not FLOW_LEASE_DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(str(FLOW_LEASE_DB_PATH), timeout=5)
        try:
            conn.execute("DELETE FROM active_flow_leases WHERE expires_at <= ?", (time.time(),))
            row = conn.execute("SELECT 1 FROM active_flow_leases LIMIT 1").fetchone()
            return row is not None
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        # During proxy startup the table may not exist yet; never turn a DB
        # read race into an unguarded rotation.
        if "no such table" not in str(exc).lower():
            log.warning("Unable to inspect active stream leases: %s", exc)
        return True


def get_public_ip() -> Optional[str]:
    """Fetches current public IP using Chrome TLS impersonation."""
    try:
        from curl_cffi import requests
        resp = requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=5)
        if resp.status_code == 200:
            return resp.json().get("ip")
    except Exception:
        try:
            from curl_cffi import requests
            resp = requests.get("https://ifconfig.me/ip", impersonate="chrome124", timeout=5)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            return None


def get_public_ip_via_proxy(proxy: Dict[str, str]) -> Optional[str]:
    """Fetches current public IP using a specific proxy."""
    try:
        from curl_cffi import requests
        resp = requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=10, proxies=proxy)
        if resp.status_code == 200:
            return resp.json().get("ip")
    except Exception:
        try:
            from curl_cffi import requests
            resp = requests.get("https://ifconfig.me/ip", impersonate="chrome124", timeout=10, proxies=proxy)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            return None
    return None


IP_HISTORY_LIMIT = 25
ip_history: List[Dict[str, Any]] = []

def get_ip_location(ip: str) -> Dict[str, str]:
    """Fetches country, flag emoji, and location details for a given IP."""
    if not ip:
        return {"country": "Unknown", "countryCode": "UN", "flag": "🌐"}
    try:
        from curl_cffi import requests
        resp = requests.get(f"http://ip-api.com/json/{ip}", impersonate="chrome124", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            country_code = data.get("countryCode", "UN")
            # Generate flag emoji from country code
            flag = "".join(chr(127397 + ord(c)) for c in country_code) if len(country_code) == 2 else "🌐"
            return {
                "country": data.get("country", "Unknown"),
                "countryCode": country_code,
                "city": data.get("city", ""),
                "flag": flag
            }
    except Exception:
        pass
    return {"country": "Unknown", "countryCode": "UN", "flag": "🌐"}

# -----------------------------------------------------------------------------
# mihomo (Clash Meta) external-controller API client
# -----------------------------------------------------------------------------
def mihomo_api_headers() -> Dict[str, str]:
    headers = {"User-Agent": "opencode-ip-mihomo/2.0"}
    if MIHOMO_SECRET:
        headers["Authorization"] = f"Bearer {MIHOMO_SECRET}"
    return headers

def mihomo_get(path: str) -> Optional[Dict[str, Any]]:
    """GET mihomo external-controller API."""
    if not MIHOMO_API_URL:
        return None
    try:
        from curl_cffi import requests
        resp = requests.get(f"{MIHOMO_API_URL}{path}", headers=mihomo_api_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
        log.debug(f"mihomo GET {path} -> HTTP {resp.status_code}")
    except Exception as e:
        log.debug(f"mihomo GET {path} failed: {e}")
    return None

def mihomo_put(path: str, payload: Dict[str, Any]) -> bool:
    """PUT mihomo external-controller API (e.g. switch group node)."""
    if not MIHOMO_API_URL:
        return False
    try:
        from curl_cffi import requests
        resp = requests.put(
            f"{MIHOMO_API_URL}{path}",
            json=payload,
            headers=mihomo_api_headers(),
            timeout=10,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        log.debug(f"mihomo PUT {path} failed: {e}")
    return False

def mihomo_group_info() -> Optional[Dict[str, Any]]:
    """Full info of the rotation group (type/all/now)."""
    return mihomo_get(f"/proxies/{MIHOMO_GROUP}")

def mihomo_group_nodes() -> List[str]:
    """All node names in the rotation group (in subscription order)."""
    data = mihomo_group_info()
    if not data:
        return []
    return data.get("all", [])

def mihomo_group_type() -> str:
    """Group type: Select / LoadBalance / URLTest / Fallback."""
    data = mihomo_group_info()
    if not data:
        return ""
    return str(data.get("type", ""))

def mihomo_current_node() -> Optional[str]:
    """Currently selected node in the rotation group."""
    data = mihomo_group_info()
    if not data:
        return None
    return data.get("now")

def mihomo_provider_proxies() -> List[Dict[str, Any]]:
    """Parse /providers/proxies response into a flat list of proxy dicts.

    mihomo may return providers as a dict {name: {...}} (newer versions) or a
    list [...] (older versions) — handle both, and tolerate missing keys.
    """
    providers = mihomo_get("/providers/proxies")
    if not providers:
        return []
    raw = providers.get("providers", [])
    out: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        # newer mihomo: {"providers": {"airport1": {...}, "airport2": {...}}}
        for prov in raw.values():
            if isinstance(prov, dict):
                for p in prov.get("proxies", []):
                    if isinstance(p, dict):
                        out.append(p)
    elif isinstance(raw, list):
        # older mihomo: {"providers": [{"proxies": [...]}, ...]}
        for prov in raw:
            if isinstance(prov, dict):
                for p in prov.get("proxies", []):
                    if isinstance(p, dict):
                        out.append(p)
    return out

def mihomo_alive_nodes() -> List[str]:
    """Nodes marked alive by mihomo health checks (empty = trust group list)."""
    # 优先从 proxy-providers 读取存活状态；读不到时返回空列表表示不筛选
    alive: List[str] = []
    for p in mihomo_provider_proxies():
        if p.get("alive") is True:
            alive.append(p.get("name", ""))
    return [name for name in alive if name]

def mihomo_good_nodes() -> List[str]:
    """Nodes that are alive AND within the latency budget (MIHOMO_MAX_LATENCY).

    Reads each proxy's health-check delay from mihomo; nodes above the latency
    ceiling are treated as unusable for the interactive-agent workload.
    0 = latency unlimited (falls back to alive-only filtering).
    """
    good: List[str] = []
    skipped_latency = 0
    for p in mihomo_provider_proxies():
        name = p.get("name", "")
        if not name or p.get("alive") is not True:
            continue
        if MIHOMO_MAX_LATENCY > 0:
            delay = p.get("delay")
            # delay 可能是 None（尚未测出），不误杀，仅排除明确超标的
            if isinstance(delay, (int, float)) and delay > MIHOMO_MAX_LATENCY:
                skipped_latency += 1
                continue
            good.append(name)
    if skipped_latency:
        log.info(f"mihomo: skipped {skipped_latency} node(s) exceeding latency limit {MIHOMO_MAX_LATENCY}ms.")
    return good

def mihomo_drop_connections() -> bool:
    """Force-close all connections so a load-balance group re-picks a node on next request."""
    if not MIHOMO_API_URL:
        return False
    try:
        from curl_cffi import requests
        resp = requests.delete(f"{MIHOMO_API_URL}/connections", headers=mihomo_api_headers(), timeout=10)
        return resp.status_code in (200, 204)
    except Exception as e:
        log.debug(f"mihomo DELETE /connections failed: {e}")
    return False

def _is_excluded_node(name: str) -> bool:
    """True if node name matches an info-placeholder keyword (e.g. 剩余流量/套餐到期)."""
    return any(kw and kw in name for kw in MIHOMO_EXCLUDE_KEYWORDS)

def mihomo_rotation_candidates() -> List[str]:
    """Return real, currently usable ROTATOR nodes in configured order."""
    nodes = [
        name for name in mihomo_group_nodes()
        if name and name not in {"DIRECT", "REJECT", "GLOBAL"} and not _is_excluded_node(name)
    ]
    if not nodes:
        return []
    provider_nodes = mihomo_provider_proxies()
    if not provider_nodes:
        return nodes
    good = set(mihomo_good_nodes())
    return [name for name in nodes if name in good]


def mihomo_switch_next_node() -> Optional[str]:
    """Switch ROTATOR to its next usable node and return that node name."""
    gtype = mihomo_group_type()
    if gtype == "LoadBalance":
        log.info(f"mihomo: group '{MIHOMO_GROUP}' is load-balance; dropping connections to force node rotation.")
        if mihomo_drop_connections():
            return mihomo_current_node() or "load-balance"
        log.error("mihomo: failed to drop connections for load-balance rotation.")
        return None

    candidates = mihomo_rotation_candidates()
    if not candidates:
        log.warning("mihomo: ROTATOR has no usable subscription nodes.")
        return None
    current = mihomo_current_node() or ""
    try:
        index = candidates.index(current)
    except ValueError:
        index = -1
    next_node = candidates[(index + 1) % len(candidates)]
    log.info(f"mihomo: switching '{MIHOMO_GROUP}' from '{current}' -> '{next_node}'")
    if mihomo_put(f"/proxies/{MIHOMO_GROUP}", {"name": next_node}):
        return next_node
    log.error(f"mihomo: failed to switch to node '{next_node}'.")
    return None


def is_mihomo_enabled() -> bool:
    return bool(MIHOMO_API_URL)

def is_admin() -> bool:
    if os.name != "nt":
        return True
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False

def elevate() -> None:
    # mihomo rotation needs no WARP/network-driver privileges. Portable Windows
    # builds explicitly disable this legacy elevation path.
    if os.environ.get("DISABLE_ELEVATION", "").lower() in ("1", "true", "yes") or MIHOMO_API_URL:
        return
    if os.name == "nt" and not is_admin():
        log.info("Requesting administrative privileges...")
        params = subprocess.list2cmdline(sys.argv)
        try:
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
        except Exception as e:
            log.error(f"Failed to elevate: {e}")
        sys.exit()

def get_warp_bin() -> str:
    path = shutil.which("warp-cli")
    if path:
        return path
    candidates = [
        r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe",
        r"C:\Program Files (x86)\Cloudflare\Cloudflare WARP\warp-cli.exe",
        "/usr/bin/warp-cli",
        "/usr/local/bin/warp-cli",
        "/bin/warp-cli",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return "warp-cli"

# -----------------------------------------------------------------------------
# WARP Controller with IP Verification & Auto-Recycle Trigger
# -----------------------------------------------------------------------------
def _write_ip_history_to_db(new_ip: str, loc: Dict[str, str], timestamp_str: str, reason: str) -> None:
    """Persist an IP rotation event into the shared SQLite metrics DB."""
    try:
        db_path = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
                (new_ip, loc.get("country", "Unknown"), loc.get("flag", "🌐"), timestamp_str, reason)
            )
            cursor.execute(
                "DELETE FROM ip_history WHERE id NOT IN (SELECT id FROM ip_history ORDER BY id DESC LIMIT ?)",
                (IP_HISTORY_LIMIT,),
            )
            conn.commit()
            conn.close()
    except Exception as err:
        log.error(f"Failed to write IP rotation to SQLite DB: {err}")

def _commit_egress_rotation(new_ip: str, reason: str) -> None:
    global _current_ip, rotation_count
    _current_ip = new_ip
    rotation_count += 1
    loc = get_ip_location(new_ip)
    timestamp_str = time.strftime("%H:%M:%S", time.localtime())
    ip_history.append({
        "ip": new_ip,
        "country": loc.get("country", "Unknown"),
        "flag": loc.get("flag", "🌐"),
        "timestamp": timestamp_str,
        "reason": reason,
    })
    if len(ip_history) > IP_HISTORY_LIMIT:
        ip_history.pop(0)
    _write_ip_history_to_db(new_ip, loc, timestamp_str, reason)


def _activate_proxy_egress(proxy_url: str, reason: str) -> bool:
    mapping = {"http": proxy_url, "https": proxy_url}
    new_ip = get_public_ip_via_proxy(mapping)
    if not new_ip:
        failures = proxy_pool.mark_proxy_failure(proxy_url)
        log.warning("Proxy rotation candidate %s failed; fail_count=%s.", proxy_url, failures)
        return False
    proxy_pool.mark_proxy_success(proxy_url)
    proxy_pool.set_active_proxy(proxy_url, verified_ip=new_ip, reason=reason)
    _commit_egress_rotation(new_ip, f"{reason} (proxy)")
    log.info("Custom proxy rotation successful: %s -> %s", proxy_url, new_ip)
    return True


def _activate_mihomo_egress(reason: str, remaining: int) -> bool:
    node = mihomo_switch_next_node()
    if not node:
        return False
    time.sleep(MIHOMO_VERIFY_SLEEP)
    new_ip = get_public_ip_via_proxy({"http": MIHOMO_OUTBOUND_PROXY, "https": MIHOMO_OUTBOUND_PROXY})
    if not new_ip:
        log.warning("mihomo node '%s' did not produce a verified public IP.", node)
        return False
    proxy_pool.set_mihomo_active(
        verified_ip=new_ip,
        reason=reason,
        hold=True,
        mihomo_remaining=remaining,
    )
    _commit_egress_rotation(new_ip, f"{reason} (mihomo:{node})")
    log.info("mihomo rotation successful: %s -> %s", node, new_ip)
    return True


def rotate_combined_egress(reason: str) -> Optional[bool]:
    """Rotate custom proxies first, then ROTATOR nodes, as one logical pool.

    Returns ``None`` only when neither custom proxies nor mihomo is configured,
    allowing the legacy WARP implementation to run.
    """
    proxies = proxy_pool.read_proxy_list()
    available = proxy_pool.eligible_proxies(proxies)
    mihomo_nodes = mihomo_rotation_candidates() if is_mihomo_enabled() else []
    if not proxies and not mihomo_nodes:
        return None

    route = proxy_pool.routing_snapshot()
    mode = route.get("mode", "proxy" if available else "mihomo")

    # While traversing ROTATOR, finish its usable node set before wrapping back
    # to the first healthy custom proxy. With no custom proxies, ROTATOR cycles forever.
    if mode == "mihomo" and route.get("hold_mihomo") is True:
        remaining = int(route.get("mihomo_remaining") or 0)
        if available and remaining <= 0:
            for candidate in available:
                if _activate_proxy_egress(candidate, reason):
                    return True
            available = proxy_pool.eligible_proxies(proxies)
        if mihomo_nodes:
            next_remaining = max(0, remaining - 1) if available else max(0, len(mihomo_nodes) - 1)
            return _activate_mihomo_egress(reason, next_remaining)

    # Start/continue the custom proxy section. Failed/cooling proxies are skipped.
    if available:
        current = route.get("active_proxy") if mode == "proxy" else None
        candidate, wrapped = proxy_pool.next_proxy_rotation(current, proxies)
        if candidate and wrapped and mihomo_nodes:
            return _activate_mihomo_egress(reason, max(0, len(mihomo_nodes) - 1))
        tried = set()
        while candidate and candidate not in tried:
            tried.add(candidate)
            if _activate_proxy_egress(candidate, reason):
                return True
            candidate, wrapped = proxy_pool.next_proxy_rotation(candidate, proxies)
            if wrapped and mihomo_nodes:
                return _activate_mihomo_egress(reason, max(0, len(mihomo_nodes) - 1))

    # All custom proxies are cooling/unavailable: automatically fall back to ROTATOR.
    if mihomo_nodes:
        proxy_pool.set_mihomo_active(reason="all custom proxies unavailable", hold=False)
        return _activate_mihomo_egress(reason, max(0, len(mihomo_nodes) - 1))

    # Proxy-only installation: wrap around the healthy pool indefinitely.
    available = proxy_pool.eligible_proxies(proxies)
    for candidate in available:
        if _activate_proxy_egress(candidate, reason):
            return True
    return False


def rotate_warp(reason: str = "Triggered") -> bool:
    global _current_ip, rotation_count
    with rotation_lock:
        with flow_lock:
            if active_flows_count > 0 or has_active_flow_leases():
                log.info("IP rotation skipped — an active streaming flow lease is in progress.")
                return False

            old_ip = _current_ip or get_public_ip()
            log.info(f"Initiating guaranteed IP rotation... (Reason: {reason} | Current IP: {old_ip})")

            managed_result = rotate_combined_egress(reason)
            if managed_result is not None:
                return managed_result

            # Try local WARP CLI rotation first (legacy mode)
            warp_bin = get_warp_bin()
            if shutil.which(warp_bin) or os.path.exists(warp_bin):
                max_attempts = 4
                for attempt in range(1, max_attempts + 1):
                    try:
                        log.info(f"WARP rotation attempt {attempt}/{max_attempts}...")
                        subprocess.run([warp_bin, "--accept-tos", "disconnect"], capture_output=True, text=True, timeout=10, check=False)
                        time.sleep(1)

                        subprocess.run([warp_bin, "--accept-tos", "registration", "delete"], capture_output=True, text=True, timeout=10, check=False)
                        time.sleep(1)
                        subprocess.run([warp_bin, "--accept-tos", "registration", "new"], capture_output=True, text=True, timeout=10, check=False)
                        time.sleep(1)

                        res = subprocess.run([warp_bin, "--accept-tos", "connect"], capture_output=True, text=True, timeout=10, check=False)

                        if res.returncode == 0:
                            time.sleep(3)
                            new_ip = get_public_ip()

                            if new_ip and new_ip != old_ip:
                                _current_ip = new_ip
                                rotation_count += 1
                                loc = get_ip_location(new_ip)

                                timestamp_str = time.strftime("%H:%M:%S", time.localtime())
                                ip_history.append({
                                    "ip": new_ip,
                                    "country": loc.get("country", "Unknown"),
                                    "flag": loc.get("flag", "🌐"),
                                    "timestamp": timestamp_str,
                                    "reason": reason
                                })
                                if len(ip_history) > IP_HISTORY_LIMIT:
                                    ip_history.pop(0)

                                try:
                                    db_path = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))
                                    if db_path.exists():
                                        conn = sqlite3.connect(str(db_path))
                                        cursor = conn.cursor()
                                        cursor.execute(
                                            "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
                                            (new_ip, loc.get("country", "Unknown"), loc.get("flag", "🌐"), timestamp_str, reason)
                                        )
                                        cursor.execute(
                                            "DELETE FROM ip_history WHERE id NOT IN (SELECT id FROM ip_history ORDER BY id DESC LIMIT ?)",
                                            (IP_HISTORY_LIMIT,),
                                        )
                                        conn.commit()
                                        conn.close()
                                except Exception as err:
                                    log.error(f"Failed to write IP rotation to SQLite DB: {err}")

                                log.info(f"Guaranteed WARP IP rotation successful! New Verified IP: {new_ip} {loc.get('flag')} ({loc.get('country')}) (Total Rotations: {rotation_count})")

                                if rotation_count >= AUTO_RECYCLE_THRESHOLD:
                                    log.warning(f"Auto-recycle threshold reached ({rotation_count}/{AUTO_RECYCLE_THRESHOLD}). Triggering container refresh...")
                                    trigger_container_recycle()

                                return True
                            else:
                                log.warning(f"Attempt {attempt}: Assigned IP ({new_ip}) was identical to old IP ({old_ip}). Retrying fresh registration...")
                    except FileNotFoundError:
                        log.error(f"Cloudflare WARP CLI ('{warp_bin}') was not found. Please install Cloudflare WARP and add warp-cli to PATH.")
                        break
                    except Exception as e:
                        log.error(f"Error during WARP rotation attempt {attempt}: {e}")
                        time.sleep(1)
            else:
                log.warning("WARP CLI not available locally. Trying remote rotator service...")

            # Try remote rotator service as fallback
            rotator_endpoints = ["http://warp-rotator:8001/rotate", "http://127.0.0.1:8001/rotate"]
            for endpoint in rotator_endpoints:
                try:
                    req = Request(endpoint, data=b"", headers={"User-Agent": "rotator-fallback"}, method="POST")
                    with urlopen(req, timeout=35) as resp:
                        if resp.status == 200:
                            res_data = json.loads(resp.read().decode("utf-8"))
                            if res_data.get("status") == "success":
                                _current_ip = res_data.get("verified_ip", _current_ip)
                                log.info(f"Rotation via remote rotator service ({endpoint}) successful. Verified IP: {_current_ip}")
                                return True
                except Exception:
                    pass

            # Try proxy rotation as final fallback
            log.warning("WARP and remote rotator unavailable. Attempting proxy rotation...")
            proxy = get_next_proxy()
            if proxy:
                new_ip = get_public_ip_via_proxy(proxy)
                if new_ip and new_ip != old_ip:
                    _current_ip = new_ip
                    rotation_count += 1
                    loc = get_ip_location(new_ip)

                    timestamp_str = time.strftime("%H:%M:%S", time.localtime())
                    ip_history.append({
                        "ip": new_ip,
                        "country": loc.get("country", "Unknown"),
                        "flag": loc.get("flag", "🌐"),
                        "timestamp": timestamp_str,
                        "reason": f"{reason} (via proxy)"
                    })
                    if len(ip_history) > IP_HISTORY_LIMIT:
                        ip_history.pop(0)

                    log.info(f"Proxy IP rotation successful! New Verified IP: {new_ip} {loc.get('flag')} ({loc.get('country')}) (Total Rotations: {rotation_count})")
                    return True
                else:
                    log.warning("Proxy rotation failed to provide a different IP.")
            else:
                log.warning("No proxies available for rotation.")

            log.error("All IP rotation methods failed (WARP, remote rotator, proxy).")
            return False

def trigger_container_recycle():
    """Trigger the legacy container recycle only when running in its container."""
    if MIHOMO_API_URL or getattr(sys, "frozen", False):
        log.info("Skipping legacy container recycle in mihomo/portable mode.")
        return
    try:
        subprocess.Popen(["python3", "manager.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        log.error(f"Failed to trigger auto-recycle: {e}")

def handle_rate_limit(attempt: int, initial_delay: int, max_retries: int) -> bool:
    delay = initial_delay * (2 ** (attempt - 1))
    log.warning(f"HTTP 429 Rate Limit detected! Retry attempt {attempt}/{max_retries} — Backoff delay: {delay}s")
    time.sleep(delay)
    return rotate_warp(reason=f"HTTP 429 - Attempt {attempt}")

# -----------------------------------------------------------------------------
# Background Monitors
# -----------------------------------------------------------------------------
def health_check_loop(endpoint: str, interval: int, initial_delay: int, max_retries: int, stop_event: threading.Event) -> None:
    log.info(f"Health check monitor started. Endpoint: {endpoint} (Interval: {interval}s)")
    retry_count = 0

    while not stop_event.is_set():
        try:
            if is_mihomo_enabled() or proxy_pool.read_proxy_list():
                # 检测当前统一出口：代理池模式检测当前 SOCKS/HTTP 代理，
                # ROTATOR 模式才检测 mihomo，避免无关的 mihomo 故障误切代理池。
                from curl_cffi import requests
                route = proxy_pool.routing_snapshot()
                active_proxy = route.get("active_proxy") if route.get("mode") == "proxy" else None
                outbound = active_proxy or MIHOMO_OUTBOUND_PROXY
                resp = requests.get(
                    endpoint,
                    proxies={"http": outbound, "https": outbound},
                    impersonate="chrome124",
                    timeout=10,
                )
                ok = resp.status_code in (200, 204)
                if active_proxy:
                    if ok:
                        proxy_pool.mark_proxy_success(active_proxy)
                    else:
                        proxy_pool.mark_proxy_failure(active_proxy)
                if ok:
                    retry_count = 0
                else:
                    # 当前节点探测异常（非 200/204，如 403/429 等）→ 换节点
                    retry_count += 1
                    if retry_count <= max_retries:
                        log.warning(f"Health check via mihomo returned HTTP {resp.status_code}. Triggering node rotation ({retry_count}/{max_retries})...")
                        rotate_warp(reason=f"Health check HTTP {resp.status_code}")
                    else:
                        log.error(f"Health check still failing after {max_retries} rotations. Pausing 30s.")
                        time.sleep(30)
                        retry_count = 0
            else:
                req = Request(endpoint, headers={"User-Agent": "WARP-Guard/1.0"}, method="HEAD")
                with urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        retry_count = 0
        except HTTPError as e:
            if e.code == 429:
                retry_count += 1
                if retry_count <= max_retries:
                    handle_rate_limit(retry_count, initial_delay, max_retries)
                else:
                    log.error(f"Maximum retry attempts ({max_retries}) reached. Pausing health check for 30s.")
                    time.sleep(30)
                    retry_count = 0
        except Exception as e:
            if is_mihomo_enabled() or proxy_pool.read_proxy_list():
                route = proxy_pool.routing_snapshot()
                if route.get("mode") == "proxy" and route.get("active_proxy"):
                    proxy_pool.mark_proxy_failure(route.get("active_proxy"))
                # 当前出口完全连不上目标 → 立即换节点
                retry_count += 1
                if retry_count <= max_retries:
                    log.warning(f"Health check via mihomo failed ({e}). Triggering node rotation ({retry_count}/{max_retries})...")
                    rotate_warp(reason=f"Health check failed: {type(e).__name__}")
                else:
                    log.error(f"Health check failing after {max_retries} rotations. Pausing 30s.")
                    time.sleep(30)
                    retry_count = 0
            else:
                log.debug(f"Health check error: {e}")

        stop_event.wait(interval)

def periodic_rotation_loop(interval: int, stop_event: threading.Event) -> None:
    if interval <= 0:
        return

    log.info(f"Periodic IP rotator started. (Interval: {interval}s)")
    while not stop_event.is_set():
        if stop_event.wait(interval):
            break
        if has_active_flow_leases():
            log.info("Scheduled IP rotation deferred — an active streaming flow lease is in progress.")
            continue
        rotate_warp(reason="Scheduled Interval")

def start_rotator_http_server():
    """Starts a lightweight HTTP server inside warp-rotator container on port 8001 to handle remote rotate requests."""
    try:
        from fastapi import FastAPI
        import uvicorn
        
        rotator_app = FastAPI()
        
        @rotator_app.get("/health")
        def http_health():
            return {"status": "healthy", "current_ip": _current_ip, "rotations": rotation_count}

        @rotator_app.post("/rotate")
        def http_rotate():
            success = rotate_warp(reason="Remote HTTP Dashboard Trigger")
            return {"status": "success" if success else "failed", "verified_ip": _current_ip, **proxy_pool.routing_snapshot()}
        
        @rotator_app.get("/status")
        def http_status():
            return {"current_ip": _current_ip, "rotations": rotation_count, "history": ip_history, **proxy_pool.routing_snapshot()}

        uvicorn.run(rotator_app, host="0.0.0.0", port=8001, log_level="warning")
    except Exception as e:
        log.error(f"Failed to start rotator HTTP listener: {e}")

def _cleanup_warp():
    warp_bin = get_warp_bin()
    if not shutil.which(warp_bin) and not os.path.exists(warp_bin):
        return
    log.info("Disconnecting WARP and cleaning up...")
    try:
        subprocess.run([warp_bin, "--accept-tos", "disconnect"], capture_output=True, text=True, timeout=10, check=False)
        subprocess.run([warp_bin, "--accept-tos", "registration", "delete"], capture_output=True, text=True, timeout=10, check=False)
    except Exception as e:
        log.warning(f"Error during WARP cleanup: {e}")
    log.info("WARP cleanup complete.")

def main() -> None:
    elevate()
    load_proxy_list()
    global _current_ip
    initial_proxy = proxy_pool.select_active_proxy(proxy_pool.read_proxy_list())
    if initial_proxy:
        _current_ip = get_public_ip_via_proxy({"http": initial_proxy, "https": initial_proxy})
        if _current_ip:
            proxy_pool.mark_proxy_success(initial_proxy)
            proxy_pool.set_active_proxy(initial_proxy, verified_ip=_current_ip, reason="startup")
        else:
            proxy_pool.mark_proxy_failure(initial_proxy)
    if not _current_ip and is_mihomo_enabled():
        _current_ip = get_public_ip_via_proxy({"http": MIHOMO_OUTBOUND_PROXY, "https": MIHOMO_OUTBOUND_PROXY})
        proxy_pool.set_mihomo_active(verified_ip=_current_ip, reason="startup", hold=False)
    elif not _current_ip:
        _current_ip = get_public_ip()
    log.info(f"Starting IP Rotator Node... Initial Verified Public IP: {_current_ip}")

    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        log.warning(f"Received signal {signum}, shutting down rotator...")
        stop_event.set()
        _cleanup_warp()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    health_thread = threading.Thread(target=health_check_loop, args=(CHECK_ENDPOINT, CHECK_INTERVAL, INITIAL_RETRY_DELAY, MAX_RETRIES, stop_event), daemon=True)
    periodic_thread = threading.Thread(target=periodic_rotation_loop, args=(PERIODIC_ROTATION_INTERVAL, stop_event), daemon=True)
    http_thread = threading.Thread(target=start_rotator_http_server, daemon=True)

    health_thread.start()
    periodic_thread.start()
    http_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down Rotator...")
        stop_event.set()
        _cleanup_warp()

if __name__ == "__main__":
    main()
